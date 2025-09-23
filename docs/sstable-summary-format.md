# SSTable Summary.db File Format Specification

**Research Date:** 2025-09-22
**Researcher:** Research Agent
**Source:** Apache Cassandra documentation, ScyllaDB documentation, real data analysis
**Cassandra Version Compatibility:** 3.0+, 5.0+

## Executive Summary

This document provides the definitive specification for the SSTable Summary.db file format used by Apache Cassandra. The research was conducted through analysis of official documentation, source code examination, and real binary data analysis from Cassandra 5.0 SSTables.

**KEY FINDING**: The current implementation in `src/storage/sstable/summary_reader.rs:254` uses an **incorrect format assumption**. The actual Cassandra format follows the ScyllaDB-documented specification with different header layout and entry structure.

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

**Total Header Size**: 20 bytes

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

## Format Differences from Current Implementation

### Current Implementation Issues

1. **Wrong Header Format**: Expects `version(u32), entry_count(u32), sampling_rate(u32), min_token(i64), max_token(i64), data_size(u64), checksum(u32)`
2. **Wrong Entry Format**: Expects `key_len(u16), partition_key(var), token(i64), index_offset(u64), position(u32)`
3. **Missing Offset Table**: Doesn't account for the offset table before entries
4. **Incorrect Data Types**: Uses wrong field types and sizes

### Correct Header Fields

| Field | Type | Size | Description |
|-------|------|------|-------------|
| min_index_interval | be32 | 4 | Minimum partitions between index entries |
| entries_count | be32 | 4 | Number of summary entries |
| summary_entries_size | be64 | 8 | Size of entries block in bytes |
| sampling_level | be32 | 4 | Sampling level (1-128) |
| size_at_full_sampling | be32 | 4 | Entries at full sampling |

### Correct Entry Format

1. **Offset Table**: Array of `uint32` offsets (little-endian) to locate each entry
2. **Entries**: Variable-length keys followed by `be64` position
3. **No embedded tokens**: Token values are not stored in summary entries
4. **No key length prefix**: Key boundaries determined by offset differences

## Implementation Recommendations

### 1. Update Header Parser

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryHeader {
    pub min_index_interval: u32,
    pub entries_count: u32,
    pub summary_entries_size: u64,
    pub sampling_level: u32,
    pub size_at_full_sampling: u32,
}

fn parse_summary_header(input: &[u8]) -> IResult<&[u8], SummaryHeader> {
    let (input, min_index_interval) = be_u32(input)?;
    let (input, entries_count) = be_u32(input)?;
    let (input, summary_entries_size) = be_u64(input)?;
    let (input, sampling_level) = be_u32(input)?;
    let (input, size_at_full_sampling) = be_u32(input)?;

    Ok((input, SummaryHeader {
        min_index_interval,
        entries_count,
        summary_entries_size,
        sampling_level,
        size_at_full_sampling,
    }))
}
```

### 2. Update Entry Parser

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummaryEntry {
    pub partition_key: Vec<u8>,
    pub position: u64,  // Position in index file
}

fn parse_summary_entries(input: &[u8], header: &SummaryHeader) -> IResult<&[u8], Vec<SummaryEntry>> {
    // Parse offset table (little-endian)
    let (mut input, offsets) = count(le_u32, header.entries_count as usize)(input)?;

    let entries_data_start = input;
    let mut entries = Vec::with_capacity(header.entries_count as usize);

    for i in 0..header.entries_count as usize {
        let start_offset = offsets[i] as usize;
        let end_offset = if i + 1 < offsets.len() {
            offsets[i + 1] as usize
        } else {
            header.summary_entries_size as usize - (offsets.len() * 4)
        };

        let key_len = end_offset - start_offset - 8; // Subtract 8 bytes for position
        let entry_data = &entries_data_start[start_offset..end_offset];

        let (_, entry) = parse_single_entry(entry_data, key_len)?;
        entries.push(entry);
    }

    Ok((input, entries))
}

fn parse_single_entry(input: &[u8], key_len: usize) -> IResult<&[u8], SummaryEntry> {
    let (input, partition_key) = take(key_len)(input)?;
    let (input, position) = be_u64(input)?;

    Ok((input, SummaryEntry {
        partition_key: partition_key.to_vec(),
        position,
    }))
}
```

### 3. Parse Serialized Keys

```rust
fn parse_serialized_keys(input: &[u8]) -> IResult<&[u8], (Vec<u8>, Vec<u8>)> {
    let (input, first_key) = parse_serialized_key(input)?;
    let (input, last_key) = parse_serialized_key(input)?;
    Ok((input, (first_key, last_key)))
}

fn parse_serialized_key(input: &[u8]) -> IResult<&[u8], Vec<u8>> {
    let (input, size) = be_u32(input)?;
    let (input, key) = take(size)(input)?;
    Ok((input, key.to_vec()))
}
```

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