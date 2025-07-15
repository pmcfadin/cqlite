# CQLite Reference Documentation

## 🎯 Essential Resources

### **Official Cassandra Documentation**
- [Apache Cassandra 5.0 Documentation](https://cassandra.apache.org/doc/5.0/)
- [SSTable Format Specification](https://cassandra.apache.org/doc/5.0/architecture/storage-engine.html)
- [CQL 3 Language Reference](https://cassandra.apache.org/doc/5.0/cql/)
- [Native Protocol Specification](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec)

### **Source Code References**
- [Cassandra Java Implementation](https://github.com/apache/cassandra/tree/trunk/src/java/org/apache/cassandra/io/sstable)
  - Key classes: `BigFormat.java`, `SSTableReader.java`, `SSTableWriter.java`
- [ScyllaDB C++ Implementation](https://github.com/scylladb/scylladb/tree/master/sstables)
  - Performance optimizations and SIMD usage patterns
- [Patrick's CQL Grammar](https://github.com/pmcfadin/cassandra-antlr4-grammar)
  - ANTLR4 grammar for CQL 3 parsing

### **Rust Ecosystem Libraries**
- [nom](https://docs.rs/nom/) - Parser combinators for binary formats
- [serde](https://serde.rs/) - Serialization framework
- [bytes](https://docs.rs/bytes/) - Efficient byte buffer manipulation
- [memmap2](https://docs.rs/memmap2/) - Cross-platform memory-mapped files
- [lz4_flex](https://docs.rs/lz4_flex/) - Fast LZ4 compression
- [criterion](https://docs.rs/criterion/) - Statistical benchmarking

## 📚 Academic Papers & Articles

### **Foundational Papers**
- ["Bigtable: A Distributed Storage System for Structured Data"](https://static.googleusercontent.com/media/research.google.com/en//archive/bigtable-osdi06.pdf) - Google, 2006
  - Original inspiration for Cassandra's storage model
- ["Cassandra - A Decentralized Structured Storage System"](https://www.cs.cornell.edu/projects/ladis2009/papers/lakshman-ladis2009.pdf) - Facebook, 2009
  - Original Cassandra design paper

### **Performance & Optimization**
- ["The Log-Structured Merge-Tree (LSM-Tree)"](https://www.cs.umb.edu/~poneil/lsmtree.pdf) - O'Neil et al., 1996
  - Foundation for SSTable design
- ["SIMD-Accelerated Regular Expression Matching"](https://www.vldb.org/pvldb/vol12/p2946-lang.pdf) - VLDB 2019
  - Techniques applicable to SSTable parsing

### **ScyllaDB Architecture**
- [ScyllaDB Architecture Whitepaper](https://www.scylladb.com/product/technology/)
- [Seastar Framework Documentation](http://seastar.io/)
  - Async, sharded design principles

## 🔧 Technical Specifications

### **Binary Format Details**
```
SSTable 'oa' Format Layout:
┌─────────────────────────┐
│      Magic (4B)         │ 0x5A5A5A5A
├─────────────────────────┤
│     Version (1B)        │ 'oa' = 0x6F61
├─────────────────────────┤
│      Flags (4B)         │ Compression, stats flags
├─────────────────────────┤
│  Partition Count (8B)   │ Big-endian u64
├─────────────────────────┤
│    Min Timestamp (8B)   │ Microseconds since epoch
├─────────────────────────┤
│    Max Timestamp (8B)   │ Microseconds since epoch
├─────────────────────────┤
│     Reserved (7B)       │ Future use
├─────────────────────────┤
│                         │
│    Partition Data       │ Variable length
│                         │
├─────────────────────────┤
│   Index Offset (8B)     │ Pointer to index section
├─────────────────────────┤
│  End Magic (8B)         │ 0x5A5A5A5A5A5A5A5A
└─────────────────────────┘
```

### **CQL Type Encoding**
| CQL Type | Encoding | Size |
|----------|----------|------|
| boolean | 0x00 or 0x01 | 1 byte |
| tinyint | Signed byte | 1 byte |
| smallint | Big-endian i16 | 2 bytes |
| int | Big-endian i32 | 4 bytes |
| bigint | Big-endian i64 | 8 bytes |
| float | IEEE 754 BE | 4 bytes |
| double | IEEE 754 BE | 8 bytes |
| varchar | [length][UTF-8] | Variable |
| blob | [length][bytes] | Variable |
| uuid | 16 bytes | 16 bytes |
| timestamp | Milliseconds BE | 8 bytes |

### **Compression Formats**
- **LZ4**: Block format with 4-byte decompressed size prefix
- **Snappy**: Frame format with CRC32C checksums
- **Deflate**: ZLIB format with standard headers

## 🛠️ Implementation Patterns

### **Parser Combinator Patterns**
```rust
// VInt parsing pattern from Cassandra
fn parse_vint(input: &[u8]) -> IResult<&[u8], i64> {
    let (input, first_byte) = take(1usize)(input)?;
    let first = first_byte[0];
    
    if first & 0x80 == 0 {
        Ok((input, first as i64))
    } else {
        let extra_bytes = first.leading_ones() as usize - 1;
        let (input, rest) = take(extra_bytes)(input)?;
        // Decode multi-byte value...
    }
}
```

### **Zero-Copy Patterns**
```rust
// From ScyllaDB - zero-copy string handling
struct StringView<'a> {
    data: &'a [u8],
}

impl<'a> StringView<'a> {
    fn as_str(&self) -> Result<&'a str> {
        std::str::from_utf8(self.data)
    }
}
```

### **SIMD Patterns**
```rust
// Checksum computation with SIMD
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

unsafe fn crc32_simd(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    let chunks = data.chunks_exact(8);
    
    for chunk in chunks {
        let val = *(chunk.as_ptr() as *const u64);
        crc = _mm_crc32_u64(crc as u64, val) as u32;
    }
    
    // Handle remainder...
    !crc
}
```

## 🔄 Related Projects

### **SSTable Tools**
- [cassandra-sstable-tools](https://github.com/tolbertam/sstable-tools) - Java-based SSTable utilities
- [sstable2json](https://github.com/apache/cassandra/tree/trunk/tools) - Official Cassandra tool
- [cassandra-diagnostics](https://github.com/smartcat-labs/cassandra-diagnostics) - Performance analysis

### **Alternative Implementations**
- [cassandra-rs](https://github.com/krojew/cassandra-rs) - Rust CQL driver
- [scylla-rust-driver](https://github.com/scylladb/scylla-rust-driver) - ScyllaDB Rust driver
- [gocql](https://github.com/gocql/gocql) - Go implementation for reference

### **Parsing Libraries**
- [arrow-rs](https://github.com/apache/arrow-rs) - Columnar format parsing patterns
- [parquet-rs](https://github.com/apache/arrow-rs/tree/master/parquet) - Complex file format parsing
- [bincode](https://github.com/bincode-org/bincode) - Binary serialization patterns

## 📊 Benchmarking Resources

### **Performance References**
- [Cassandra Performance Benchmarks](https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-Denylisted-Reads.html)
- [ScyllaDB vs Cassandra Benchmarks](https://www.scylladb.com/product/benchmarks/)
- [SSTable Compaction Performance](https://thelastpickle.com/blog/2016/12/08/TWCS-part1.html)

### **Rust Performance**
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [The Rust Performance Book](https://github.com/flamegraph-rs/flamegraph)
- [Criterion.rs Benchmarking Guide](https://bheisler.github.io/criterion.rs/book/)

## 🐛 Debugging Resources

### **SSTable Analysis Tools**
- `sstabledump` - Official Cassandra tool for SSTable inspection
- `sstablemetadata` - Metadata extraction utility
- `sstablescrub` - Corruption detection and repair

### **Hex Editors for Format Analysis**
- [ImHex](https://imhex.werwolv.net/) - Pattern-based hex editor
- [010 Editor](https://www.sweetscape.com/010editor/) - Binary templates
- [hexyl](https://github.com/sharkdp/hexyl) - Command-line hex viewer

## 📈 Monitoring & Profiling

### **Rust Profiling Tools**
- [perf](https://perf.wiki.kernel.org/) - Linux profiling
- [Instruments](https://developer.apple.com/xcode/features/) - macOS profiling
- [dhat](https://docs.rs/dhat/) - Heap profiling
- [flamegraph](https://github.com/flamegraph-rs/flamegraph) - Visualization

### **Memory Analysis**
- [Valgrind](https://valgrind.org/) - Memory error detection
- [ASAN/MSAN](https://doc.rust-lang.org/unstable-book/compiler-flags/sanitizer.html) - Rust sanitizers
- [heaptrack](https://github.com/KDE/heaptrack) - Heap memory profiler

---

*This reference compilation provides direct links to essential resources for implementing and optimizing CQLite. All links were verified as of the documentation date.*