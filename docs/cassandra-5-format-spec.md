# Cassandra 5+ 'oa' Format Specification

**CRITICAL: Zero-Tolerance Format Compliance Document**

> This specification documents the exact byte-level format requirements for Apache Cassandra 5+ SSTable 'oa' format compatibility. Every detail must be implemented exactly as specified to ensure 100% compatibility.

## Table of Contents

1. [Format Overview](#format-overview)
2. [File Structure](#file-structure)
3. [BigFormat 'oa' Specification](#bigformat-oa-specification)
4. [BTI Format Specification](#bti-format-specification)
5. [Compression Algorithms](#compression-algorithms)
6. [Data Encoding](#data-encoding)
7. [Index Structures](#index-structures)
8. [Validation Requirements](#validation-requirements)

---

## Format Overview

### Cassandra 5.0 Format Versions

| Format | Version | Description | Status |
|--------|---------|-------------|--------|
| BIG | 'oa' | Enhanced BigFormat with improved metadata | **REQUIRED** |
| BTI | 'da' | Big Trie-Indexed format with trie-based indexing | **OPTIONAL** |

### Critical Format Requirements

- **Magic Number**: `0x6F61_0000` ("oa" + version bytes)
- **Endianness**: Big-endian for all multi-byte values
- **Checksums**: CRC32 mandatory for data integrity
- **VInt Encoding**: Cassandra-specific variable-length integers
- **UTF-8 Strings**: All text data must be valid UTF-8

---

## File Structure

### SSTable Components (Both BIG and BTI)

```
SSTable Directory:
├── <keyspace>-<table>-<generation>-Data.db       # Row data
├── <keyspace>-<table>-<generation>-Index.db      # Partition index
├── <keyspace>-<table>-<generation>-Summary.db    # Index summary (BIG only)
├── <keyspace>-<table>-<generation>-Filter.db     # Bloom filter
├── <keyspace>-<table>-<generation>-Statistics.db # Table statistics
├── <keyspace>-<table>-<generation>-CompressionInfo.db # Compression metadata
├── <keyspace>-<table>-<generation>-Digest.crc32  # File integrity
└── <keyspace>-<table>-<generation>-TOC.txt       # Table of contents
```

### BTI Additional Components

```
BTI Format Additions:
├── <keyspace>-<table>-<generation>-Partitions.db # BTI partition index
└── <keyspace>-<table>-<generation>-Rows.db       # BTI row index
```

---

## BigFormat 'oa' Specification

### Magic Number and Version Detection

```
Header Structure (6 bytes):
[0x6F] [0x61] [0x00] [0x00] [0x00] [0x01]
  │      │      │      │      │      │
  │      │      └─────────────────────┴─ Version: 0x0001
  │      └─ Format identifier: 'a'
  └─ Format identifier: 'o'
```

### 'oa' Format Improvements

#### 1. Improved Min/Max Handling
- **Requirement**: More precise min/max timestamp tracking
- **Implementation**: 64-bit microsecond precision timestamps
- **Validation**: Min ≤ Max, both within valid timestamp range

#### 2. Partition Level Deletion Presence Marker
- **Location**: Statistics.db metadata section
- **Format**: Single bit flag in partition metadata
- **Purpose**: Indicates presence of partition-level tombstones

#### 3. Key Range Support (CASSANDRA-18134)
- **Feature**: Enhanced partition key range metadata
- **Format**: Binary-encoded min/max partition keys
- **Requirement**: Lexicographically ordered for efficient range queries

#### 4. Long Deletion Time (TTL Overflow Prevention)
- **Change**: 64-bit deletion time instead of 32-bit
- **Format**: Signed 64-bit microseconds since epoch
- **Critical**: Prevents TTL overflow beyond 2038

#### 5. Token Space Coverage
- **Purpose**: Track token range coverage for virtual nodes
- **Format**: Array of (start_token, end_token) pairs
- **Encoding**: VInt-encoded token values

### Data.db Structure ('oa' format)

```
┌─────────────────────────────────────────────────────────────┐
│                    SSTable Data File                        │
├─────────────────────────────────────────────────────────────┤
│ Header (32 bytes)                                           │
│ ┌─────────────┬─────────────┬─────────────┬─────────────┐   │
│ │Magic(4)     │Version(2)   │Flags(4)     │Reserved(22) │   │
│ │0x6F610000   │0x0001       │See below    │Zero-filled  │   │
│ └─────────────┴─────────────┴─────────────┴─────────────┘   │
├─────────────────────────────────────────────────────────────┤
│ Metadata Section (Variable length)                          │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Partition Count (VInt)                                  │ │
│ │ Min Timestamp (8 bytes, signed)                        │ │
│ │ Max Timestamp (8 bytes, signed)                        │ │
│ │ Token Coverage Array (VInt count + token pairs)        │ │
│ │ Compression Info Offset (8 bytes)                      │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Partition Data Blocks                                       │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Partition 1                                             │ │
│ │ ├─ Partition Key (VInt length + bytes)                  │ │
│ │ ├─ Deletion Info (if present)                           │ │
│ │ ├─ Static Row (if present)                              │ │
│ │ └─ Clustering Rows                                      │ │
│ │    ├─ Row Header                                        │ │
│ │    ├─ Clustering Key                                    │ │
│ │    ├─ Row Timestamp (VInt delta from min)              │ │
│ │    ├─ Row TTL (VInt, if present)                       │ │
│ │    └─ Column Data                                       │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Footer (16 bytes)                                           │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Index Offset (8 bytes)                                  │ │
│ │ CRC32 Checksum (4 bytes)                               │ │
│ │ Magic Verification (4 bytes) = 0x6F610000              │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Header Flags (4 bytes)

```
Bit Layout:
31    24 23    16 15     8 7      0
┌───────┬────────┬────────┬────────┐
│Reserve│Compress│Features│ Basic  │
└───────┴────────┴────────┴────────┘

Basic Flags (bits 0-7):
- 0x01: Has compression
- 0x02: Has static columns
- 0x04: Has regular columns
- 0x08: Has complex columns (collections, UDTs)
- 0x10: Has partition-level deletion
- 0x20: Has TTL data
- 0x40: Reserved
- 0x80: Reserved

Feature Flags (bits 8-15):
- 0x0100: Key range support enabled
- 0x0200: Long deletion time format
- 0x0400: Token space coverage present
- 0x0800: Enhanced min/max timestamps
- 0x1000-0x8000: Reserved for future use

Compression Flags (bits 16-23):
- 0x010000: LZ4 compression
- 0x020000: Snappy compression
- 0x040000: Deflate compression
- 0x080000: Custom compression
- 0x100000-0x800000: Reserved

Reserved (bits 24-31): Must be zero
```

---

## BTI Format Specification

### BTI Overview

**BTI (Big Trie-Indexed)** is an alternative indexing format that uses trie data structures for more efficient partition and row lookups.

### Key Characteristics

- **Version**: 'da' (0x6461_0000)
- **Shared Components**: Data.db, Statistics.db, Filter.db, CompressionInfo.db
- **Unique Components**: Partitions.db, Rows.db
- **Index Summary**: Not used (replaced by trie efficiency)
- **Key Cache**: Not needed (trie provides direct lookup)

### Byte-Comparable Key Encoding

**Critical Requirement**: All keys must be encoded in byte-comparable format where lexicographic comparison of unsigned bytes produces the same ordering as typed comparison.

```rust
// Example: Converting a composite key to byte-comparable format
fn encode_byte_comparable(key: &CompositeKey) -> Vec<u8> {
    let mut result = Vec::new();
    
    for component in &key.components {
        match component {
            KeyComponent::Text(s) => {
                // UTF-8 strings are naturally byte-comparable
                result.extend_from_slice(s.as_bytes());
                result.push(0x00); // Separator
            }
            KeyComponent::Integer(i) => {
                // Convert to unsigned with sign bit adjustment
                let unsigned = (*i as u64) ^ 0x8000_0000_0000_0000;
                result.extend_from_slice(&unsigned.to_be_bytes());
            }
            KeyComponent::Uuid(uuid) => {
                // UUIDs need special byte-order conversion
                result.extend_from_slice(&encode_uuid_byte_comparable(uuid));
            }
        }
    }
    
    result
}
```

### Trie Node Types

BTI uses four node types optimized for different key distribution patterns:

#### 1. PAYLOAD_ONLY Node
```
Byte Layout:
[Type: 0x00] [Payload Length: VInt] [Payload Data: Variable]
```

#### 2. SINGLE Node
```
Byte Layout:
[Type: 0x01] [Transition Byte: 1] [Child Offset: VInt] [Payload?: Optional]
```

#### 3. SPARSE Node
```
Byte Layout:
[Type: 0x02] [Transition Count: VInt]
[Transition 1: byte + offset] ... [Transition N: byte + offset]
[Payload?: Optional]
```

#### 4. DENSE Node
```
Byte Layout:
[Type: 0x03] [Start Byte: 1] [Bitmap: 32 bytes] [Offsets: VInt array]
[Payload?: Optional]
```

### Partitions.db Structure

```
┌─────────────────────────────────────────────────────────────┐
│                BTI Partition Index File                     │
├─────────────────────────────────────────────────────────────┤
│ Header (16 bytes)                                           │
│ ┌─────────────┬─────────────┬─────────────┬─────────────┐   │
│ │Magic(4)     │Version(2)   │Root Offset  │Flags(4)     │   │
│ │0x6461_0000  │0x0001       │(8 bytes)    │See below    │   │
│ └─────────────┴─────────────┴─────────────┴─────────────┘   │
├─────────────────────────────────────────────────────────────┤
│ Trie Nodes (Page-aligned)                                   │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Node 1 (Root)                                           │ │
│ │ ├─ Node Type (1 byte)                                   │ │
│ │ ├─ Transition Table                                     │ │
│ │ ├─ Child Offsets (VInt array)                          │ │
│ │ └─ Payload (Data file position + size)                 │ │
│ │                                                         │ │
│ │ Node 2...N (Children)                                   │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Footer (8 bytes)                                            │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Total Nodes (4 bytes)                                   │ │
│ │ CRC32 Checksum (4 bytes)                               │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Rows.db Structure

```
┌─────────────────────────────────────────────────────────────┐
│                   BTI Row Index File                        │
├─────────────────────────────────────────────────────────────┤
│ Header (Similar to Partitions.db)                          │
├─────────────────────────────────────────────────────────────┤
│ Per-Partition Row Tries                                     │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Partition 1 Row Trie                                    │ │
│ │ ├─ Trie Root Offset (8 bytes)                           │ │
│ │ ├─ Block Granularity (4 bytes, default 16KB)           │ │
│ │ └─ Row Block Index Trie                                 │ │
│ │    ├─ Clustering Key → Block Position                   │ │
│ │    └─ Block Separators (not min/max keys)              │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## Compression Algorithms

### LZ4 Compression

**Algorithm**: LZ4 with Cassandra-specific parameters

#### Required Parameters
```yaml
LZ4 Configuration:
  block_size: 4096, 8192, 16384, 32768, 65536 # bytes
  compression_level: 1-12 (1=fastest, 12=best ratio)
  checksum: CRC32
  block_independence: true
  content_checksum: true
```

#### Block Format
```
LZ4 Block Structure:
[Block Size: 4 bytes] [Compressed Data: Variable] [CRC32: 4 bytes]
```

#### Implementation Requirements
- **Library**: Compatible with LZ4 v1.9.3+
- **Block Independence**: Each block compressible independently
- **Checksums**: CRC32 for each block + overall content checksum
- **Error Handling**: Fail fast on checksum mismatch

### Snappy Compression

**Algorithm**: Google Snappy with streaming format

#### Required Parameters
```yaml
Snappy Configuration:
  block_size: 4096, 8192, 16384, 32768, 65536 # bytes
  streaming_format: true
  checksum: CRC32C
  compression_level: N/A (fixed algorithm)
```

#### Block Format
```
Snappy Block Structure:
[Magic: 4 bytes = 0x73NaP*] [Block Size: VInt] [Compressed Data] [CRC32C: 4 bytes]
```

### Deflate Compression

**Algorithm**: zlib deflate with streaming

#### Required Parameters
```yaml
Deflate Configuration:
  compression_level: 1-9 (1=fastest, 9=best ratio)
  window_size: 15 # bits (32KB window)
  memory_level: 8
  strategy: Z_DEFAULT_STRATEGY
  checksum: Adler32
```

---

## Data Encoding

### Variable-Length Integers (VInt)

**Critical**: Cassandra uses a specific VInt encoding that differs from protobuf.

#### Encoding Rules

1. **Length Determination**: Count leading zeros in first byte
2. **Value Extraction**: Remaining bits contain the actual value
3. **Sign Extension**: Two's complement for negative numbers

#### Implementation

```rust
fn decode_vint(bytes: &[u8]) -> Result<(i64, usize)> {
    if bytes.is_empty() {
        return Err(Error::UnexpectedEof);
    }
    
    let first_byte = bytes[0];
    let leading_zeros = first_byte.leading_zeros() as usize;
    
    let length = if leading_zeros >= 8 { 1 } else { leading_zeros + 1 };
    
    if bytes.len() < length {
        return Err(Error::UnexpectedEof);
    }
    
    let mut value: i64;
    
    if length == 1 {
        // Single byte: 0xxxxxxx or 1xxxxxxx
        value = (first_byte & 0x7F) as i64;
        if first_byte & 0x80 != 0 {
            value |= !0x7F; // Sign extend
        }
    } else {
        // Multi-byte: extract value bits
        let mask = (1u8 << (8 - leading_zeros)) - 1;
        value = (first_byte & mask) as i64;
        
        for &byte in &bytes[1..length] {
            value = (value << 8) | (byte as i64);
        }
        
        // Sign extension for negative numbers
        let sign_bit_pos = (length * 8) - leading_zeros - 1;
        if sign_bit_pos < 64 && (value >> sign_bit_pos) & 1 != 0 {
            let sign_extend_mask = !((1i64 << (sign_bit_pos + 1)) - 1);
            value |= sign_extend_mask;
        }
    }
    
    Ok((value, length))
}
```

#### Test Cases

```
Value    | Encoding (hex)     | Length
---------|-------------------|-------
0        | 00                | 1
1        | 01                | 1
63       | 3F                | 1
64       | C0 40             | 2
127      | C0 7F             | 2
128      | C0 80             | 2
-1       | FF                | 1
-64      | C0                | 1
-65      | BF BF             | 2
```

### String Encoding

**Format**: [Length: VInt] [UTF-8 Bytes]

#### Requirements
- **Encoding**: Valid UTF-8 only
- **Length**: Byte length (not character count)
- **Null Handling**: Length = -1 for null strings
- **Empty Strings**: Length = 0, no data bytes

### Timestamp Encoding

**Format**: Microseconds since Unix epoch (1970-01-01 00:00:00 UTC)

#### Requirements
- **Type**: Signed 64-bit integer
- **Range**: 1677-09-21 to 2262-04-11
- **Precision**: Microseconds
- **Encoding**: Big-endian

### UUID Encoding

**Format**: 16 bytes in network byte order

```
UUID Structure:
[Time Low: 4 bytes] [Time Mid: 2 bytes] [Time High: 2 bytes]
[Clock Seq: 2 bytes] [Node: 6 bytes]
```

#### Type 1 UUID (TimeUUID)
- **Timestamp**: 60-bit, 100-nanosecond units since 1582-10-15
- **Clock Sequence**: 14-bit counter
- **Node**: 48-bit MAC address or random

---

## Index Structures

### Bloom Filter

**Purpose**: Fast negative lookups for partition keys

#### Parameters
```yaml
Bloom Filter Configuration:
  hash_functions: 3-8 (optimal based on expected elements)
  false_positive_rate: 0.01 (1%)
  hash_algorithm: MurmurHash3 (128-bit)
  bit_array_size: Calculated based on element count and FPR
```

#### Hash Function Implementation

**Critical**: Must use exact MurmurHash3 implementation

```rust
fn bloom_hash(key: &[u8], seed: u32) -> u128 {
    // MurmurHash3 128-bit implementation
    // MUST match Apache Cassandra's implementation exactly
    murmur3_x64_128(key, seed)
}

fn bloom_contains(filter: &BloomFilter, key: &[u8]) -> bool {
    let hash = bloom_hash(key, filter.seed);
    let hash1 = (hash & 0xFFFF_FFFF_FFFF_FFFF) as u64;
    let hash2 = (hash >> 64) as u64;
    
    for i in 0..filter.hash_count {
        let bit_index = (hash1.wrapping_add(i * hash2)) % filter.bit_count;
        if !filter.get_bit(bit_index) {
            return false;
        }
    }
    true
}
```

### Statistics.db Format

```
┌─────────────────────────────────────────────────────────────┐
│                  Statistics Database                        │
├─────────────────────────────────────────────────────────────┤
│ Header (16 bytes)                                           │
│ ┌─────────────┬─────────────┬─────────────┬─────────────┐   │
│ │Magic(4)     │Version(2)   │Entry Count  │Flags(4)     │   │
│ │0x53544154   │0x0001       │(VInt)       │TBD          │   │
│ └─────────────┴─────────────┴─────────────┴─────────────┘   │
├─────────────────────────────────────────────────────────────┤
│ Global Statistics                                           │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Partition Count (VInt)                                  │ │
│ │ Row Count (8 bytes)                                     │ │
│ │ Min Timestamp (8 bytes, microseconds)                  │ │
│ │ Max Timestamp (8 bytes, microseconds)                  │ │
│ │ Min Deletion Time (8 bytes, microseconds) - NEW 'oa'   │ │
│ │ Max Deletion Time (8 bytes, microseconds) - NEW 'oa'   │ │
│ │ Compression Ratio (8 bytes, IEEE 754 double)           │ │
│ │ Estimated Key Count (VInt)                              │ │
│ │ Estimated Key Size (VInt, bytes)                       │ │
│ │ Estimated Value Size (VInt, bytes)                     │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Token Range Coverage (NEW 'oa')                            │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Range Count (VInt)                                      │ │
│ │ Range 1: [Start Token: VInt] [End Token: VInt]          │ │
│ │ Range 2: [Start Token: VInt] [End Token: VInt]          │ │
│ │ ... (additional ranges)                                 │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Partition Key Range (NEW 'oa')                             │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Min Key Length (VInt)                                   │ │
│ │ Min Key Data (Variable)                                 │ │
│ │ Max Key Length (VInt)                                   │ │
│ │ Max Key Data (Variable)                                 │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Column Statistics (Per-column metadata)                    │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Column Count (VInt)                                     │ │
│ │ Column 1:                                               │ │
│ │ ├─ Name (VString)                                       │ │
│ │ ├─ Type (VString)                                       │ │
│ │ ├─ Cardinality Estimate (VInt)                         │ │
│ │ ├─ Min Value Length (VInt)                              │ │
│ │ ├─ Max Value Length (VInt)                              │ │
│ │ ├─ Null Count (VInt)                                    │ │
│ │ └─ Has Tombstones (1 byte boolean)                     │ │
│ │ ... (additional columns)                                │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Deletion Presence Markers (NEW 'oa')                       │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Has Partition Deletions (1 byte boolean)               │ │
│ │ Has Row Deletions (1 byte boolean)                     │ │
│ │ Has Cell Deletions (1 byte boolean)                    │ │
│ │ Has Range Tombstones (1 byte boolean)                  │ │
│ └─────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│ Footer (8 bytes)                                            │
│ ┌─────────────────────────────────────────────────────────┐ │
│ │ Total Size (4 bytes)                                    │ │
│ │ CRC32 Checksum (4 bytes)                               │ │
│ └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## Validation Requirements

### Format Validation Checklist

#### Header Validation
- [ ] Magic number exactly matches expected value
- [ ] Version is supported (0x0001 for 'oa')
- [ ] Flags are valid and consistent
- [ ] Reserved fields are zero
- [ ] Header checksum is correct

#### Structure Validation
- [ ] All VInt values are properly encoded
- [ ] String lengths match actual byte counts
- [ ] UTF-8 strings are valid
- [ ] File size matches expected content
- [ ] All offsets point to valid locations

#### Data Validation
- [ ] Timestamps are within valid range
- [ ] UUIDs have correct format
- [ ] Compression blocks have valid checksums
- [ ] Bloom filter parameters are consistent
- [ ] Index entries are properly sorted

#### Cross-Reference Validation
- [ ] Index entries match actual data positions
- [ ] Statistics match actual data content
- [ ] Compression info matches compressed blocks
- [ ] TOC file lists all present components

### Error Handling Requirements

#### Corruption Detection
- **CRC32 Mismatch**: Immediate failure, no recovery
- **Invalid Magic**: Reject file, not compatible format
- **Truncated Data**: Attempt partial recovery if possible
- **Invalid UTF-8**: Reject string, treat as corruption

#### Recovery Strategies
- **Partial Index**: Rebuild from data file if possible
- **Missing Statistics**: Recalculate from available data
- **Corrupted Blocks**: Skip block, continue with remainder
- **Invalid Compression**: Attempt uncompressed fallback

### Testing Requirements

#### Compatibility Testing
- Test against real Cassandra 5+ generated files
- Verify round-trip encoding/decoding
- Test with all supported compression algorithms
- Validate edge cases (empty tables, large partitions)

#### Performance Testing
- Benchmark parsing speed vs file size
- Memory usage under various conditions
- Compression/decompression performance
- Index lookup performance

---

## Critical Implementation Notes

### ⚠️ Zero-Tolerance Requirements

1. **Byte-Perfect Compatibility**: Any deviation from this specification will cause incompatibility
2. **Endianness**: All multi-byte values MUST be big-endian
3. **Checksums**: CRC32 validation is mandatory for data integrity
4. **VInt Encoding**: Must exactly match Cassandra's implementation
5. **String Encoding**: Only valid UTF-8 strings are allowed
6. **Timestamp Range**: Must handle full 64-bit microsecond range
7. **Compression**: Must support exact algorithm parameters
8. **Magic Numbers**: Must match exactly, including reserved bytes

### 🔧 Implementation Priority

1. **Phase 1**: Basic 'oa' format reading (Data.db, Index.db)
2. **Phase 2**: Compression support (LZ4, Snappy)
3. **Phase 3**: BTI format support (Partitions.db, Rows.db)
4. **Phase 4**: Advanced features (Statistics.db, Filter.db)
5. **Phase 5**: Write support and validation tools

### 📊 Validation Tools Required

- Hex dump analyzer for manual inspection
- Format deviation detector
- Byte-level comparison with reference files
- Automated regression testing framework
- Performance benchmarking suite

---

**End of Specification**

*This document represents the definitive format specification for Cassandra 5+ compatibility. All implementations must adhere to these exact requirements to ensure 100% compatibility with Apache Cassandra.*