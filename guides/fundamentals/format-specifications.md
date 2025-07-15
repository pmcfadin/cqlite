# SSTable Format Specifications

## 🎯 Cassandra 5 SSTable Format ('oa' version)

### **Core File Components**
Every SSTable in Cassandra 5 consists of these components:

```
SSTable Files:
├── Data.db              # Actual row data with partition ordering
├── Index.db             # Partition key → data file position mapping
├── Summary.db           # Sampled index entries for memory efficiency
├── Statistics.db        # Metadata: timestamps, tombstones, cardinality
├── CompressionInfo.db   # Compression chunk metadata and offsets
├── Filter.db            # Bloom filter for partition key existence
├── Digest.crc32         # CRC-32 integrity checksum
└── TOC.txt              # Table of contents listing all components
```

### **Data.db Structure**
```
[Header: 32 bytes]
├── Magic Number: 4 bytes (0x5A5A5A5A)
├── Format Version: 1 byte ('oa' for Cassandra 5)
├── Flags: 4 bytes (compression, stats, etc.)
├── Partition Count: 8 bytes
├── Timestamp Range: 16 bytes (min/max)
└── Reserved: 7 bytes

[Partition Data Blocks]
├── Partition 1: Variable length
├── Partition 2: Variable length
└── ...

[Footer: 16 bytes]
├── Index Offset: 8 bytes
└── Magic Number: 8 bytes
```

### **Index.db Structure**
```
[Index Header]
├── Entry Count: 4 bytes
├── Key Size Threshold: 4 bytes
└── Flags: 4 bytes

[Index Entries] (sorted by partition key)
├── Entry 1: [Key Length][Key][Position][Size]
├── Entry 2: [Key Length][Key][Position][Size]
└── ...
```

### **Summary.db Structure**
```
[Summary Header]
├── Sample Rate: 4 bytes (every Nth index entry)
├── Entry Count: 4 bytes
└── Reserved: 8 bytes

[Sampled Entries]
├── Sample 1: [Key][Index Position]
├── Sample 2: [Key][Index Position]
└── ...
```

## ⚡ ScyllaDB Format Differences

### **Compatibility Matrix**
| Component | Cassandra 5 | ScyllaDB | CQLite Target |
|-----------|-------------|----------|---------------|
| Data.db | 'oa' format | 'oa' compatible | ✅ Support both |
| Index.db | Standard | Optimized layout | ✅ Read both |
| Compression | LZ4/Snappy/Deflate | Same + custom | ✅ Standard algorithms |
| Bloom Filter | Standard implementation | SIMD optimized | ✅ Standard + SIMD |

### **ScyllaDB Optimizations**
- **Vectorized operations** for bulk processing
- **Lock-free index updates** during reads
- **Memory-mapped file access** with zero-copy
- **SIMD checksums** for integrity validation

## 📊 Binary Encoding Specifications

### **Variable Length Integers (VInt)**
```rust
// Cassandra VInt encoding
fn decode_vint(bytes: &[u8]) -> (i64, usize) {
    // First byte determines length
    let first = bytes[0];
    if first & 0x80 == 0 {
        // Single byte: 0xxxxxxx
        (first as i64, 1)
    } else {
        // Multi-byte: 1xxxxxxx + additional bytes
        let extra_bytes = first.leading_ones() as usize - 1;
        // Decode remaining bytes...
    }
}
```

### **String Encoding**
```
[Length: VInt][UTF-8 Bytes]
```

### **Collection Encoding**
```
[Element Count: VInt]
[Element 1: Type-specific encoding]
[Element 2: Type-specific encoding]
...
```

## 🔄 Version Evolution

### **Format Version History**
| Version | Cassandra | Key Changes |
|---------|-----------|-------------|
| ma | 3.0-3.11 | Original big format |
| na | 4.0-4.1 | Improved metadata |
| oa | 5.0+ | Enhanced statistics, BTI support |

### **CQLite Strategy**
- **Start with 'oa' format only** (Cassandra 5+)
- **No backward compatibility** to older versions
- **Future-proof design** for post-5.0 versions
- **Universal compatibility** with ScyllaDB 'oa' files

## 🛠️ Implementation Guidelines

### **Critical Requirements**
1. **Exact format compliance** - any deviation breaks compatibility
2. **Endianness handling** - network byte order for multi-byte values
3. **Checksum validation** - always verify data integrity
4. **Version detection** - fail fast on unsupported formats

### **Performance Considerations**
1. **Memory-map large files** for efficient random access
2. **Parse index on demand** rather than loading everything
3. **Cache decompressed blocks** with LRU eviction
4. **Use SIMD** for bulk checksum operations

### **Error Handling**
1. **Corrupt data detection** through checksum mismatches
2. **Partial file recovery** when possible
3. **Graceful degradation** for unknown metadata
4. **Clear error messages** for debugging

---

*This specification covers the essential format details needed for CQLite implementation, based on comprehensive analysis of Cassandra 5 source code and ScyllaDB optimizations.*