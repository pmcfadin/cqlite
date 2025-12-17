# SSTable Summary.db File Format Specification

**Research Date:** 2025-09-22
**Researcher:** Research Agent
**Source:** Apache Cassandra documentation, ScyllaDB documentation, real data analysis
**Cassandra Version Compatibility:** 3.0+, 5.0+

## Executive Summary

This document provides the definitive specification for the SSTable Summary.db file format used by Apache Cassandra. The research was conducted through analysis of official documentation, source code examination, and real binary data analysis from Cassandra 5.0 SSTables.

**STATUS**: As of Issue #218 (December 2025), the implementation in `src/storage/sstable/summary_reader.rs` has been **corrected** to match this specification.

## Actual Format Specification

### File Structure Overview

```c
struct summary_file {
    struct summary_header header;
    struct summary_entries_block entries_block;
    struct serialized_key first_key;
    struct serialized_key last_key;
};
```

### Header Structure (Correct Format)

```c
struct summary_header {
    be32 min_index_interval;      // Lower bound for average partitions between index entries
    be32 entries_count;           // Number of entries in summary
    be64 summary_entries_size;    // Total size of summary entries block in bytes
    be32 sampling_level;          // Sampling level (1-128, typically 128)
    be32 size_at_full_sampling;   // Entries count at full sampling
};
```

**Total Header Size**: 24 bytes (not 20 - includes full 64-bit summary_entries_size)

### Real Data Analysis

From hex dump of `nb-1-big-Summary.db`:
```
00000000: 00 00 00 80  // min_index_interval = 128
00000004: 00 00 00 08  // entries_count = 8
00000008: 00 00 00 00  // (high 32-bits of summary_entries_size)
0000000C: 00 00 00 e0  // summary_entries_size = 224 bytes
00000010: 00 00 00 80  // sampling_level = 128
00000014: 00 00 00 08  // size_at_full_sampling = 8
```

### Summary Entries Block Structure

```c
struct summary_entries_block {
    uint32 offsets[header.entries_count];     // Little-endian offsets to entries
    struct summary_entry entries[header.entries_count];
};
```

### Summary Entry Structure

```c
struct summary_entry {
    byte key[];               // Variable-length key (no length prefix)
    be64 position;           // Position in index file
};
```

**Key Points:**
- **No length prefix** for keys in entries
- Key length is determined from offset differences
- Position is a big-endian 64-bit integer
- Keys are stored contiguously

### Serialized Key Structure

```c
struct serialized_key {
    be32 size;               // Key size in bytes
    byte key[size];          // Key data
};
```

## Format Summary

### Header Fields (24 bytes)

| Field | Type | Size | Description |
|-------|------|------|-------------|
| min_index_interval | be32 | 4 | Minimum partitions between index entries |
| entries_count | be32 | 4 | Number of summary entries |
| summary_entries_size | be64 | 8 | Size of entries block in bytes |
| sampling_level | be32 | 4 | Sampling level (1-128) |
| size_at_full_sampling | be32 | 4 | Entries at full sampling |

### Entry Format

1. **Offset Table**: Array of `uint32` offsets (little-endian) to locate each entry
2. **Entries**: Variable-length keys followed by `be64` position
3. **No embedded tokens**: Token values are not stored in summary entries
4. **No key length prefix**: Key boundaries determined by offset differences

## Implementation Reference

See `cqlite-core/src/storage/sstable/summary_reader.rs` for the current implementation that follows this specification.

## Testing and Validation

### Real Data Test Cases

1. **Header Parsing Test**: Validate against actual Summary.db files
2. **Entry Parsing Test**: Verify offset table logic and key extraction
3. **Key Boundary Test**: Ensure correct key length calculation
4. **Position Validation**: Check index file position values

### Test Data Sources

- `/Users/patrick/local_projects/cqlite/test-data/datasets/sstables/test_basic/simple_table-*/nb-*-big-Summary.db`
- `/Users/patrick/local_projects/cqlite/tests/fixtures/cassandra5/minimal/simple_table/Summary.db`

## Compatibility Notes

### Cassandra Version Support

| Version | Format | Header Size | Entry Format | Status |
|---------|--------|-------------|--------------|--------|
| 3.0+ | SSTable 3.0 | 20 bytes | Offset-based | Supported |
| 4.0+ | SSTable 3.0 | 20 bytes | Offset-based | Supported |
| 5.0+ | SSTable 3.0 | 20 bytes | Offset-based | Current |

### Endianness Considerations

- **Header fields**: Big-endian (network byte order)
- **Offset table**: Little-endian (for compatibility)
- **Entry positions**: Big-endian
- **Serialized key sizes**: Big-endian

## Performance Implications

### Memory Layout
- Offset table enables O(1) entry access
- Variable-length keys reduce memory overhead
- In-memory offset caching improves lookup performance

### Parsing Strategy
1. Parse header and validate counts
2. Read offset table for entry boundaries
3. Parse entries on-demand using offsets
4. Cache frequently accessed entries

## Error Conditions

### Common Parse Failures
1. **Insufficient data**: File truncated or corrupted
2. **Invalid offset values**: Offsets beyond file boundaries
3. **Malformed entries**: Key/position data corruption
4. **Header inconsistencies**: Count mismatches

### Validation Checks
- Verify `summary_entries_size` matches actual data
- Validate offset table consistency
- Check entry boundary alignment
- Ensure position values are reasonable

## Summary

The Summary.db format is **significantly different** from the current implementation assumptions. The correct format uses:

1. **20-byte header** with min_index_interval, entries_count, summary_entries_size, sampling_level, size_at_full_sampling
2. **Little-endian offset table** for entry boundaries
3. **Variable-length entries** without length prefixes
4. **Position-only metadata** (no tokens in entries)
5. **Serialized first/last keys** at file end

This research provides the complete specification needed to implement a correct Summary.db parser that is compatible with actual Cassandra data files.