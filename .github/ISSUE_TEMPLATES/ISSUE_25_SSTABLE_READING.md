# Issue #25: 🔍 Test and validate core SSTable reading functionality

## 🎯 **Priority: HIGH** - Foundation Requirement

**Status**: Core library compiles but SSTable reading needs validation  
**Impact**: Fundamental functionality verification needed  
**Estimated Effort**: 3-4 days  
**Assigned**: TBD  

---

## 📋 **Problem Statement**

While the CQLite core library compiles successfully, we need to validate that the fundamental SSTable reading functionality works correctly with real Cassandra data files. This is the foundation that all other functionality depends on.

Current unknowns:
- Can we successfully read real Cassandra 5.x SSTable files?
- Does the format detection work across different versions?
- Are the data type parsers handling all CQL types correctly?
- Does compression/decompression work for all supported algorithms?

## ✅ **Acceptance Criteria**

### **Core Reading Functionality**
- [ ] Successfully open and read Cassandra 5.0+ SSTable files
- [ ] Correct parsing of SSTable headers and metadata
- [ ] Accurate data extraction from compressed and uncompressed files
- [ ] Proper handling of all CQL data types (text, int, uuid, collections, etc.)
- [ ] Schema information extraction from SSTable files
- [ ] Index and bloom filter reading capabilities

### **Compatibility Requirements**
- [ ] Support for Cassandra 5.0 Big format
- [ ] Support for Cassandra 5.0 BTI (Big Trie Index) format  
- [ ] Backward compatibility with Cassandra 4.x formats
- [ ] Handle various compression algorithms (LZ4, Snappy, Deflate)
- [ ] Process files with different partition and clustering key configurations

### **Data Validation**
- [ ] Verify data integrity during reading
- [ ] Detect and handle corrupted files gracefully
- [ ] Validate CQL type mappings are correct
- [ ] Ensure timestamp and TTL information is preserved
- [ ] Confirm tombstone handling works properly

## 🔧 **Technical Requirements**

### **Test Coverage Areas**

1. **Format Detection and Parsing**
   ```rust
   // Test SSTable format detection
   let format = SSTableReader::detect_format(&sstable_path)?;
   assert_eq!(format, SSTableFormat::BigFormat);
   
   // Test header parsing
   let reader = SSTableReader::open(&sstable_path, &config).await?;  
   let header = reader.get_header()?;
   assert_eq!(header.version, CassandraVersion::V5_0);
   ```

2. **Data Type Handling**
   ```rust
   // Test all CQL data types
   let test_cases = vec![
       (CqlType::Text, "test_string"),
       (CqlType::Int, 42i32),
       (CqlType::Uuid, uuid::Uuid::new_v4()),
       (CqlType::List(Box::new(CqlType::Int)), vec![1, 2, 3]),
       // ... more types
   ];
   ```

3. **Compression Support**
   ```rust
   // Test different compression algorithms
   for algorithm in [CompressionAlgorithm::LZ4, CompressionAlgorithm::Snappy] {
       let reader = create_compressed_sstable(algorithm).await?;
       let data = reader.read_all_entries().await?;
       assert!(!data.is_empty());
   }
   ```

### **Implementation Steps**

1. **Create Comprehensive Test Suite**
   - Unit tests for SSTable components
   - Integration tests with real data files
   - Property-based tests for edge cases
   - Performance benchmarks for large files

2. **Test Data Generation**
   - Generate test SSTable files using Docker Cassandra
   - Create files with various data types and sizes
   - Include compressed and uncompressed variants
   - Generate schema-rich and schema-less files

3. **Validation Framework**
   - Compare parsed data with known expected values
   - Validate against Cassandra's own output (cqlsh)
   - Cross-check with SSTable metadata files
   - Performance regression testing

### **Test Data Requirements**

#### **Basic Test Files**
- [ ] Simple table with text and int columns
- [ ] Table with UUID and timestamp columns
- [ ] Table with collection types (list, set, map)
- [ ] Table with complex composite keys
- [ ] Table with multiple clustering columns

#### **Advanced Test Files**  
- [ ] Large tables (>1GB) for performance testing
- [ ] Tables with tombstones and TTL data
- [ ] Tables with various compression algorithms
- [ ] Tables with user-defined types (UDTs)
- [ ] Tables with secondary indexes

#### **Edge Case Files**
- [ ] Empty tables and files
- [ ] Corrupted files for error handling
- [ ] Files with mixed format versions
- [ ] Files with unusual schema configurations

## 🧪 **Testing Strategy**

### **Unit Tests**
```rust
#[tokio::test]
async fn test_sstable_basic_reading() {
    let test_file = "test-data/users-simple-v5.db";
    let reader = SSTableReader::open(test_file, &Config::default()).await?;
    
    let entries = reader.stream_entries().await?;
    let mut count = 0;
    while let Some(entry) = entries.next().await? {
        count += 1;
        assert!(!entry.key.is_empty());
        assert!(!entry.values.is_empty());
    }
    assert!(count > 0);
}
```

### **Integration Tests**
```rust  
#[tokio::test]
async fn test_end_to_end_data_reading() {
    // Test complete workflow: file → parsing → data extraction
    let test_data = generate_test_sstables().await?;
    
    for (sstable_path, expected_data) in test_data {
        let reader = SSTableReader::open(&sstable_path, &Config::default()).await?;
        let actual_data = reader.read_all_entries().await?;
        
        assert_eq!(actual_data.len(), expected_data.len());
        // Compare data values...
    }
}
```

### **Performance Tests**
```rust
#[tokio::test]
async fn test_large_file_performance() {
    let large_file = "test-data/large-table-1gb.db";
    let start = Instant::now();
    
    let reader = SSTableReader::open(large_file, &Config::default()).await?;
    let entry_count = reader.count_entries().await?;
    
    let duration = start.elapsed();
    assert!(duration < Duration::from_secs(30)); // Performance threshold
    assert!(entry_count > 1_000_000); // Ensure it's actually large
}
```

## 📊 **Success Metrics**

### **Functionality Metrics**
- [ ] 100% success rate reading valid SSTable files
- [ ] 100% accuracy in data type parsing
- [ ] Support for all Cassandra 5.0+ format features
- [ ] Graceful handling of 100% of error conditions

### **Performance Metrics**
- [ ] Read throughput > 50MB/s for large files
- [ ] Memory usage < 128MB for 1GB+ files (streaming)
- [ ] Startup latency < 100ms for file opening
- [ ] CPU usage remains reasonable during processing

### **Quality Metrics**
- [ ] Unit test coverage > 95% for SSTable components
- [ ] Integration test coverage > 90% for data workflows
- [ ] Zero memory leaks during processing
- [ ] Consistent behavior across platforms

## 🔄 **Test Data Generation Plan**

### **Docker-based Generation**
```bash
# Start Cassandra 5.0 container
docker run -d cassandra:5.0

# Create test schemas and data
docker exec cassandra cqlsh -e "
    CREATE KEYSPACE test_data WITH replication = {
        'class': 'SimpleStrategy', 'replication_factor': 1
    };
    
    CREATE TABLE test_data.users (
        id UUID PRIMARY KEY,
        name TEXT,
        email TEXT,
        created_at TIMESTAMP
    );
    
    -- Insert test data
    INSERT INTO test_data.users (id, name, email, created_at) 
    VALUES (uuid(), 'Test User 1', 'test1@example.com', toTimestamp(now()));
"

# Export SSTable files
docker cp cassandra:/var/lib/cassandra/data/test_data ./test-data/
```

### **Validation Process**
1. **Generate Reference Data** - Use cqlsh to export expected values
2. **Parse with CQLite** - Read same files with our implementation  
3. **Compare Results** - Validate data matches exactly
4. **Performance Baseline** - Establish performance benchmarks

## 📖 **Documentation Needs**

- [ ] SSTable reading capabilities and limitations
- [ ] Supported Cassandra versions and formats
- [ ] Performance characteristics and tuning
- [ ] Error handling and troubleshooting guide
- [ ] Test data generation procedures

## 🚀 **Implementation Plan**

### **Phase 1: Core Validation (Days 1-2)**
1. Create basic test SSTable files
2. Implement fundamental reading tests
3. Validate format detection works
4. Test basic data type parsing

### **Phase 2: Comprehensive Testing (Days 2-3)**
1. Generate comprehensive test data set
2. Implement integration tests
3. Add performance benchmarking
4. Test error handling scenarios

### **Phase 3: Edge Cases and Polish (Days 3-4)**
1. Test edge cases and corrupted files
2. Validate cross-platform compatibility
3. Optimize performance bottlenecks
4. Complete documentation

## ⚠️ **Risk Factors**

- **High**: Format changes in Cassandra 5.0+ that aren't documented
- **Medium**: Performance issues with very large files
- **Medium**: Compatibility issues across different Cassandra versions
- **Low**: Platform-specific file handling differences

## 💡 **Dependencies**

- **Required**: Docker for test data generation
- **Required**: Access to various Cassandra versions for compatibility testing
- **Blocks**: REPL functionality (#24), Info command (#26), Query execution (#27)
- **Related**: Test infrastructure implementation

---

**Labels**: `high-priority`, `core`, `storage`, `testing`, `phase-1`  
**Milestone**: Foundation  
**Dependencies**: Compilation fixes (complete)