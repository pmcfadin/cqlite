# 🚨 SSTable Writer Cassandra Compatibility Analysis

## 📊 CRITICAL COMPATIBILITY ISSUES IDENTIFIED

### 🔴 **MAJOR ISSUES - BREAKS CASSANDRA COMPATIBILITY**

#### 1. **Wrong Magic Bytes** 
- **Current**: `[0x43, 0x51, 0x4C, 0x69, 0x74, 0x65, 0x53, 0x54]` ("CQLiteST")
- **Required**: `[0x5A, 0x5A, 0x5A, 0x5A]` (Cassandra magic)
- **Impact**: Cassandra will reject files immediately
- **Fix Priority**: CRITICAL

#### 2. **Wrong Format Version**
- **Current**: `5u32` (arbitrary number)
- **Required**: `0x6f, 0x61` ('oa' for Cassandra 5)
- **Impact**: Format version check will fail
- **Fix Priority**: CRITICAL

#### 3. **Non-Standard Header Structure**
- **Current**: Custom CQLite header format
- **Required**: Exact Cassandra 'oa' format header
- **Impact**: Header parsing will fail in Cassandra
- **Fix Priority**: CRITICAL

#### 4. **Incorrect File Layout**
- **Current**: Single .sst file with embedded components
- **Required**: Multi-file layout (Data.db, Index.db, Summary.db, etc.)
- **Impact**: Cassandra expects separate component files
- **Fix Priority**: CRITICAL

#### 5. **Wrong Binary Encoding**
- **Current**: Little-endian encoding throughout
- **Required**: Big-endian for multi-byte values (network byte order)
- **Impact**: All numeric values will be misinterpreted
- **Fix Priority**: CRITICAL

#### 6. **Non-Standard VInt Encoding**
- **Current**: Simple varint implementation
- **Required**: Cassandra-specific VInt encoding rules
- **Impact**: Variable-length fields won't decode properly
- **Fix Priority**: HIGH

#### 7. **Bloom Filter Format Incompatible**
- **Current**: Bincode serialization with custom format
- **Required**: Cassandra bloom filter binary format
- **Impact**: Bloom filter won't load in Cassandra
- **Fix Priority**: HIGH

#### 8. **Index Format Incompatible**
- **Current**: Bincode serialized index entries
- **Required**: BTI (Binary Tree Index) format
- **Impact**: Index files won't be readable
- **Fix Priority**: HIGH

#### 9. **Compression Implementation Issues**
- **Current**: Different compression parameters and output
- **Required**: Exact Cassandra compression format and parameters
- **Impact**: Compressed blocks won't decompress correctly
- **Fix Priority**: HIGH

### 🟡 **MEDIUM ISSUES - COMPATIBILITY CONCERNS**

#### 10. **Checksum Algorithm Differences**
- **Current**: CRC32 with custom polynomial
- **Required**: Cassandra-specific checksum implementation
- **Fix Priority**: MEDIUM

#### 11. **Timestamp Format**
- **Current**: Microseconds since UNIX epoch
- **Required**: Cassandra timestamp format verification
- **Fix Priority**: MEDIUM

#### 12. **Statistics Format**
- **Current**: Custom statistics structure
- **Required**: Cassandra Statistics.db format
- **Fix Priority**: MEDIUM

## 🎯 MANDATORY FIXES FOR CASSANDRA COMPATIBILITY

### **PHASE 1: CRITICAL HEADER & MAGIC FIXES**

#### Fix 1: Correct Magic Bytes
```rust
// Before (WRONG):
const SSTABLE_MAGIC: [u8; 8] = [0x43, 0x51, 0x4C, 0x69, 0x74, 0x65, 0x53, 0x54];

// After (CORRECT):
const CASSANDRA_MAGIC: [u8; 4] = [0x5A, 0x5A, 0x5A, 0x5A];
```

#### Fix 2: Correct Format Version
```rust
// Before (WRONG):
const SSTABLE_FORMAT_VERSION: u32 = 5;

// After (CORRECT):
const CASSANDRA_FORMAT_VERSION: &[u8] = b"oa"; // Cassandra 5+ format
```

#### Fix 3: Big-Endian Encoding
```rust
// Before (WRONG):
header.extend_from_slice(&value.to_le_bytes());

// After (CORRECT):
header.extend_from_slice(&value.to_be_bytes());
```

### **PHASE 2: FILE STRUCTURE OVERHAUL**

#### Fix 4: Multi-File SSTable Layout
```rust
pub struct CassandraSSTableWriter {
    data_writer: BufWriter<File>,      // Data.db
    index_writer: IndexFileWriter,     // Index.db  
    summary_writer: SummaryFileWriter, // Summary.db
    stats_writer: StatsFileWriter,     // Statistics.db
    bloom_writer: BloomFileWriter,     // Filter.db
    compression_writer: Option<CompressionWriter>, // CompressionInfo.db
    toc_writer: TOCWriter,             // TOC.txt
}
```

### **PHASE 3: COMPRESSION COMPATIBILITY**

#### Fix 5: Cassandra-Compatible Compression
```rust
impl CompressionWriter {
    pub fn compress_cassandra_compatible(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.algorithm {
            LZ4 => {
                // Use EXACT same LZ4 parameters as Cassandra
                lz4_compress_hc(data, CASSANDRA_LZ4_LEVEL)
            }
            Snappy => {
                // Use EXACT same Snappy parameters as Cassandra  
                snappy::compress_with_cassandra_params(data)
            }
            Deflate => {
                // Use EXACT same Deflate parameters as Cassandra
                deflate_compress_cassandra_compatible(data)
            }
        }
    }
}
```

### **PHASE 4: INDEX & BLOOM FILTER FIXES**

#### Fix 6: BTI Index Format
```rust
pub struct BTIIndexWriter {
    entries: Vec<BTIIndexEntry>,
    sample_rate: u32,
}

impl BTIIndexWriter {
    pub fn write_cassandra_format(&self, writer: &mut impl Write) -> Result<()> {
        // Write exact BTI format matching Cassandra
        self.write_bti_header(writer)?;
        self.write_bti_entries(writer)?;
        self.write_bti_footer(writer)?;
    }
}
```

#### Fix 7: Cassandra Bloom Filter Format
```rust
impl BloomFilter {
    pub fn serialize_cassandra_format(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        
        // Cassandra bloom filter header
        output.extend_from_slice(&self.hash_count.to_be_bytes());
        output.extend_from_slice(&self.bit_count.to_be_bytes());
        
        // Cassandra bloom filter bit array
        for word in &self.bits {
            output.extend_from_slice(&word.to_be_bytes());
        }
        
        Ok(output)
    }
}
```

## 🧪 VALIDATION FRAMEWORK REQUIREMENTS

### **Round-Trip Validation Tests**
1. **Write with CQLite → Read with Cassandra**
2. **Write with Cassandra → Read with CQLite**  
3. **Byte-for-byte comparison** of identical data
4. **Performance benchmarks** against Cassandra

### **Compatibility Test Matrix**
| Test Case | CQLite Write → Cassandra Read | Cassandra Write → CQLite Read |
|-----------|-------------------------------|-------------------------------|
| Small SSTable (1K entries) | ✅ Must Pass | ✅ Must Pass |
| Large SSTable (1M entries) | ✅ Must Pass | ✅ Must Pass |
| Compressed SSTable | ✅ Must Pass | ✅ Must Pass |
| Multiple Tables | ✅ Must Pass | ✅ Must Pass |
| Complex Data Types | ✅ Must Pass | ✅ Must Pass |

## 📈 IMPLEMENTATION PRIORITY

### **CRITICAL PATH (MUST FIX FIRST)**
1. Magic bytes and format version
2. Big-endian encoding throughout
3. Multi-file SSTable structure
4. Basic Cassandra header format

### **HIGH PRIORITY (FIX NEXT)**
1. VInt encoding compatibility
2. Compression algorithm compatibility
3. BTI index format
4. Bloom filter format

### **MEDIUM PRIORITY (OPTIMIZE LATER)**
1. Statistics format
2. Checksum algorithms
3. Performance optimizations
4. Advanced features

## ⚠️ COMPATIBILITY TESTING STRATEGY

### **Validation Tools Required**
1. **Cassandra SSTable Tools** - Use `sstabletool` to validate files
2. **Hex Diff Analysis** - Byte-by-byte comparison with reference files
3. **Round-Trip Tests** - Write/read cycles with Cassandra
4. **Performance Benchmarks** - Ensure no performance regression

### **Success Criteria**
- ✅ All CQLite-generated SSTables load successfully in Cassandra 5+
- ✅ Zero format-related errors in Cassandra logs
- ✅ Read performance within 5% of native Cassandra files
- ✅ Write performance meets or exceeds current CQLite performance

---

**NEXT STEPS**: Begin PHASE 1 implementation immediately, focusing on magic bytes, format version, and endianness fixes as these are blocking all compatibility.