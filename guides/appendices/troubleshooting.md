# CQLite Troubleshooting Guide

## 🎯 Common Issues & Solutions

### **Parse Errors**

#### **Error: "Invalid magic number"**
```rust
Error: ParseError { kind: InvalidMagic, expected: 0x5A5A5A5A, found: 0x12345678 }
```

**Causes:**
- File is not an SSTable
- Corrupted file header
- Wrong file component (e.g., trying to parse Index.db as Data.db)

**Solutions:**
```bash
# Verify file type
file MyTable-oa-1-big-Data.db

# Check magic number
xxd -l 4 MyTable-oa-1-big-Data.db
# Should show: 5a5a 5a5a

# Ensure you're parsing the correct component
cqlite parse MyTable-oa-1-big-Data.db  # Correct
cqlite parse MyTable-oa-1-big-Index.db # Wrong component
```

#### **Error: "Unsupported format version"**
```rust
Error: ParseError { kind: UnsupportedVersion, version: "ma" }
```

**Causes:**
- SSTable from older Cassandra version (<5.0)
- CQLite only supports 'oa' format

**Solutions:**
```bash
# Check SSTable version
sstablemetadata MyTable-oa-1-big-Data.db | grep "SSTable version"

# Upgrade SSTable using Cassandra tools
sstableupgrade MyTable-ma-1-big-Data.db

# Or regenerate with Cassandra 5.0+
```

#### **Error: "Checksum mismatch"**
```rust
Error: IntegrityError { expected_crc: 0xABCDEF00, actual_crc: 0x12345678 }
```

**Causes:**
- File corruption during transfer
- Disk errors
- Incomplete file

**Solutions:**
```bash
# Verify file integrity
sstablescrub keyspace table

# Compare with Digest.crc32
crc32 MyTable-oa-1-big-Data.db
cat MyTable-oa-1-big-Digest.crc32

# Re-download or copy file
rsync --checksum source/MyTable* destination/
```

### **Memory Issues**

#### **Error: "Cannot allocate memory"**
```rust
Error: SystemError { kind: OutOfMemory, size: 1073741824 }
```

**Causes:**
- Trying to load entire large SSTable into memory
- Memory leaks in user code
- System memory limits

**Solutions:**
```rust
// Use streaming API instead of loading entire file
let reader = SSTableReader::open("large-file.db")?;
for partition in reader.partitions() {
    process_partition(partition)?;
}

// Set memory limits for WASM
#[cfg(target_arch = "wasm32")]
const MAX_MEMORY: usize = 512 * 1024 * 1024; // 512MB limit
```

#### **Memory-Mapped File Errors**
```
Error: IoError { kind: InvalidInput, message: "cannot memory-map empty file" }
```

**Solutions:**
```rust
// Check file size before mapping
let metadata = std::fs::metadata(&path)?;
if metadata.len() == 0 {
    return Err(CQLiteError::EmptyFile);
}

// Handle permission errors
match unsafe { Mmap::map(&file) } {
    Ok(mmap) => Ok(mmap),
    Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
        // Fall back to regular I/O
        read_file_buffered(&file)
    }
    Err(e) => Err(e.into()),
}
```

### **Performance Issues**

#### **Slow Parsing Performance**
**Symptoms:**
- Parsing takes minutes for GB-sized files
- High CPU usage with little progress

**Diagnosis:**
```bash
# Profile the parsing
RUST_LOG=cqlite=debug cargo run --release -- parse large.db

# Generate flamegraph
cargo flamegraph --bin cqlite -- parse large.db
```

**Solutions:**
```rust
// Enable release optimizations
[profile.release]
opt-level = 3
lto = "fat"
codegen-units = 1

// Use parallel parsing for independent partitions
use rayon::prelude::*;

partitions.par_iter()
    .map(|p| parse_partition(p))
    .collect::<Result<Vec<_>, _>>()?;
```

#### **Excessive Memory Usage**
**Diagnosis:**
```bash
# Monitor memory usage
/usr/bin/time -v cqlite parse large.db

# Use heaptrack for detailed analysis
heaptrack cqlite parse large.db
heaptrack --analyze heaptrack.cqlite.12345.gz
```

**Solutions:**
```rust
// Use streaming decompression
let mut decompressor = FrameDecoder::new(compressed_reader);
let mut buffer = [0u8; 8192];
while let Ok(n) = decompressor.read(&mut buffer) {
    if n == 0 { break; }
    process_chunk(&buffer[..n])?;
}

// Clear caches periodically
if cache.len() > MAX_CACHE_ENTRIES {
    cache.clear();
}
```

### **Compression Issues**

#### **Error: "Failed to decompress block"**
```rust
Error: CompressionError { algorithm: "lz4", message: "invalid block size" }
```

**Causes:**
- Corrupted compression metadata
- Incompatible LZ4 version
- Wrong compression algorithm

**Solutions:**
```bash
# Check compression info
sstablemetadata MyTable-oa-1-big-Data.db | grep -i compress

# Verify with Cassandra's tools
sstableverify keyspace table
```

```rust
// Handle multiple compression formats
match compression_type {
    CompressionType::LZ4 => decompress_lz4(data),
    CompressionType::Snappy => decompress_snappy(data),
    CompressionType::Deflate => decompress_deflate(data),
    CompressionType::None => Ok(data.to_vec()),
}
```

### **Type Conversion Errors**

#### **Error: "Invalid UTF-8 sequence"**
```rust
Error: TypeError { expected: "Text", found: "invalid UTF-8" }
```

**Solutions:**
```rust
// Handle invalid UTF-8 gracefully
match String::from_utf8(bytes) {
    Ok(s) => CQLValue::Text(s),
    Err(_) => CQLValue::Blob(bytes), // Fall back to blob
}

// Or use lossy conversion
let text = String::from_utf8_lossy(bytes);
```

#### **Error: "Timestamp out of range"**
```rust
Error: ValueError { type: "Timestamp", value: -9223372036854775808 }
```

**Solutions:**
```rust
// Handle special timestamp values
const TIMESTAMP_MIN: i64 = -9223372036854775808;

match timestamp {
    TIMESTAMP_MIN => None, // Treat as null
    ts => Some(DateTime::from_timestamp_millis(ts)?),
}
```

### **Platform-Specific Issues**

#### **WASM: "Memory access out of bounds"**
**Solutions:**
```javascript
// Increase WASM memory
const memory = new WebAssembly.Memory({
    initial: 256,  // 16MB
    maximum: 4096  // 256MB
});

// Handle growth failures
try {
    memory.grow(16); // Grow by 1MB
} catch (e) {
    console.error("Cannot allocate more WASM memory");
}
```

#### **Windows: "File in use by another process"**
**Solutions:**
```rust
// Windows-specific file handling
#[cfg(windows)]
fn open_file_windows(path: &Path) -> Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    use winapi::um::winnt::FILE_SHARE_READ;
    
    OpenOptions::new()
        .read(true)
        .share_mode(FILE_SHARE_READ)
        .open(path)
}
```

### **FFI Issues**

#### **Python: "Symbol not found"**
```
ImportError: dlopen(cqlite.so): Symbol not found: _cqlite_parse
```

**Solutions:**
```bash
# Check symbol exports
nm -D cqlite.so | grep cqlite_

# Ensure correct linking
[lib]
crate-type = ["cdylib"]

[dependencies]
safer-ffi = { version = "0.1", features = ["python"] }
```

#### **Node.js: "Invalid ELF header"**
**Solutions:**
```json
// package.json - Use pre-built binaries
{
  "optionalDependencies": {
    "@cqlite/linux-x64": "0.1.0",
    "@cqlite/darwin-x64": "0.1.0",
    "@cqlite/win32-x64": "0.1.0"
  }
}
```

## 🔍 Debugging Techniques

### **Enable Debug Logging**
```bash
# Rust log levels
RUST_LOG=cqlite=debug cargo run

# Detailed parser traces
RUST_LOG=cqlite::parser=trace cargo run

# With timestamps
RUST_LOG=cqlite=debug RUST_LOG_STYLE=always cargo run 2>&1 | ts
```

### **Parser Debugging**
```rust
// Add debug prints in nom parsers
fn parse_partition(input: &[u8]) -> IResult<&[u8], Partition> {
    #[cfg(debug_assertions)]
    eprintln!("Parsing partition at offset {}", input.len());
    
    // Enable nom's trace feature
    #[cfg(debug_assertions)]
    return nom_trace::trace("partition", parse_partition_inner)(input);
    
    #[cfg(not(debug_assertions))]
    parse_partition_inner(input)
}
```

### **Hex Dump Analysis**
```bash
# Compare with reference implementation
xxd -g 1 -l 256 problem-file.db > cqlite-hex.txt
sstabledump problem-file.db > cassandra-out.txt

# Find differences
diff cqlite-hex.txt reference-hex.txt
```

### **Test Case Minimization**
```rust
// Create minimal reproducible test case
#[test]
fn minimal_repro() {
    let problem_data = &[
        0x5A, 0x5A, 0x5A, 0x5A,  // Magic
        0x6F, 0x61,              // Version "oa"
        // Minimal data that triggers issue
    ];
    
    let result = parse_sstable(problem_data);
    assert!(result.is_err());
    println!("Error: {:?}", result.unwrap_err());
}
```

## 📊 Performance Tuning

### **Optimization Checklist**
1. ✅ Using release build (`--release`)
2. ✅ LTO enabled in Cargo.toml
3. ✅ CPU features enabled (`RUSTFLAGS="-C target-cpu=native"`)
4. ✅ Memory mapping for large files
5. ✅ Avoiding unnecessary allocations
6. ✅ Using SIMD where available

### **Benchmark Comparison**
```bash
# Compare with baseline
cargo bench --bench parsing -- --baseline 0.1.0

# Profile specific scenarios
cargo bench --bench parsing -- --profile-time 10
```

## 🆘 Getting Help

### **Information to Provide**
When reporting issues, include:
1. CQLite version (`cqlite --version`)
2. Rust version (`rustc --version`)
3. Operating system and architecture
4. Cassandra version that created SSTable
5. Error message and stack trace
6. Minimal code to reproduce

### **Debug Build for Reports**
```bash
# Build with debug symbols
RUST_BACKTRACE=full cargo build

# Get detailed error information
RUST_BACKTRACE=full RUST_LOG=debug ./target/debug/cqlite parse problem.db 2> error.log
```

### **Community Resources**
- GitHub Issues: Report bugs and request features
- Discord: Real-time help and discussions
- Stack Overflow: Tag with `cqlite` and `cassandra`

---

*This troubleshooting guide covers the most common issues encountered when using CQLite. For issues not covered here, please check the GitHub issues or reach out to the community.*