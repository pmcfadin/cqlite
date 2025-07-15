# CQLite Testing Strategy

## 🎯 Comprehensive Testing Approach

This document outlines the multi-layered testing strategy for CQLite, ensuring reliability, performance, and compatibility with Cassandra 5.

## 🏗️ Testing Architecture

### **Test Pyramid**
```
        /\
       /  \  E2E Tests (5%)
      /    \ - Full Cassandra compatibility
     /------\ Integration Tests (20%)
    /        \ - Component interaction
   /          \ - Real SSTable parsing
  /------------\ Unit Tests (75%)
 /              \ - Core logic validation
/________________\ - Type conversions
```

## 📦 Test Data Management

### **Gold Master Test Files**
```bash
test-data/
├── cassandra5/          # Real Cassandra 5 generated files
│   ├── simple/         # Basic single-table SSTables
│   ├── complex/        # UDTs, collections, counters
│   ├── compressed/     # LZ4, Snappy, Deflate variants
│   └── edge-cases/     # Reserved words, limits
├── synthetic/          # Generated test cases
│   ├── corrupted/      # Corruption scenarios
│   ├── minimal/        # Smallest valid files
│   └── stress/         # Performance test data
└── regression/         # Bug reproduction files
```

### **Test Data Generation**
```rust
// Test data builder for creating specific scenarios
pub struct TestDataBuilder {
    schema: Schema,
    compression: CompressionType,
    row_count: usize,
}

impl TestDataBuilder {
    pub fn simple_time_series() -> Self {
        Self {
            schema: Schema::time_series_default(),
            compression: CompressionType::LZ4,
            row_count: 10_000,
        }
    }
    
    pub fn with_udt() -> Self {
        let mut schema = Schema::new("test_ks", "test_table");
        schema.add_udt_column("user_profile", UserProfileUDT::schema());
        Self { schema, ..Default::default() }
    }
    
    pub fn build(self) -> TestSSTable {
        // Generate SSTable with Docker Cassandra
        let cassandra = CassandraDocker::start();
        cassandra.create_schema(&self.schema);
        cassandra.insert_data(self.generate_data());
        cassandra.flush();
        cassandra.extract_sstable()
    }
}
```

## 🧪 Unit Testing

### **Parser Combinators**
```rust
#[cfg(test)]
mod parser_tests {
    use super::*;
    use nom::error::VerboseError;
    
    #[test]
    fn test_vint_parsing() {
        // Single byte
        assert_eq!(parse_vint(&[0x42]), Ok((&[][..], 0x42)));
        
        // Multi-byte
        assert_eq!(parse_vint(&[0x81, 0x00]), Ok((&[][..], 128)));
        
        // Edge cases
        assert_eq!(parse_vint(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]), 
                   Ok((&[][..], i32::MAX)));
    }
    
    #[test]
    fn test_partition_key_parsing() {
        let input = &[
            0x00, 0x04,  // length
            b't', b'e', b's', b't'  // "test"
        ];
        
        let (remaining, key) = parse_partition_key(input).unwrap();
        assert_eq!(remaining.len(), 0);
        assert_eq!(key.as_str(), "test");
    }
}
```

### **Type System Testing**
```rust
#[cfg(test)]
mod type_tests {
    use proptest::prelude::*;
    
    // Property-based testing for all CQL types
    proptest! {
        #[test]
        fn cql_value_serialization_roundtrip(value: CQLValue) {
            let serialized = value.serialize().unwrap();
            let deserialized = CQLValue::deserialize(&serialized).unwrap();
            prop_assert_eq!(value, deserialized);
        }
        
        #[test]
        fn timestamp_bounds(millis: i64) {
            let ts = CQLValue::Timestamp(millis);
            let serialized = ts.serialize().unwrap();
            prop_assert_eq!(serialized.len(), 8);
        }
    }
    
    // Arbitrary implementations for property testing
    impl Arbitrary for CQLValue {
        type Parameters = ();
        type Strategy = BoxedStrategy<Self>;
        
        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            prop_oneof![
                Just(CQLValue::Null),
                any::<bool>().prop_map(CQLValue::Boolean),
                any::<i32>().prop_map(CQLValue::Int),
                any::<String>().prop_map(CQLValue::Text),
                // ... all other types
            ].boxed()
        }
    }
}
```

## 🔄 Integration Testing

### **SSTable Compatibility Tests**
```rust
#[test]
fn test_real_cassandra5_sstable() {
    let test_file = TestData::load("cassandra5/simple/users-oa-1-big-Data.db");
    
    let sstable = SSTable::open(&test_file).expect("Failed to open SSTable");
    
    // Verify structure
    assert_eq!(sstable.format_version(), "oa");
    assert_eq!(sstable.compression_type(), CompressionType::LZ4);
    
    // Test partition access
    let partition = sstable.get_partition(b"user123").unwrap();
    assert_eq!(partition.row_count(), 5);
    
    // Verify data content
    let row = partition.get_row(0).unwrap();
    assert_eq!(row.get_column("name"), Some(&CQLValue::Text("Alice".into())));
}

#[test]
fn test_compression_variants() {
    for compression in &[CompressionType::None, CompressionType::LZ4, 
                        CompressionType::Snappy, CompressionType::Deflate] {
        let test_file = TestData::load(&format!("cassandra5/compressed/data-{:?}.db", compression));
        let sstable = SSTable::open(&test_file).expect("Failed to open");
        
        // All should parse to same data
        assert_eq!(sstable.partition_count(), 1000);
    }
}
```

### **Schema Evolution Tests**
```rust
#[test]
fn test_schema_evolution() {
    // V1: Original schema
    let v1_file = TestData::load("cassandra5/evolution/v1-data.db");
    let v1_sstable = SSTable::open(&v1_file).unwrap();
    
    // V2: Added column
    let v2_file = TestData::load("cassandra5/evolution/v2-added-column.db");
    let v2_sstable = SSTable::open(&v2_file).unwrap();
    
    // Should handle missing column gracefully
    let v1_partition = v1_sstable.get_partition(b"key1").unwrap();
    assert_eq!(v1_partition.get_column("new_column"), None);
    
    // V2 should have new column
    let v2_partition = v2_sstable.get_partition(b"key1").unwrap();
    assert!(v2_partition.get_column("new_column").is_some());
}
```

## 🚀 Performance Testing

### **Benchmark Suite**
```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

fn parsing_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable_parsing");
    
    for size in &[1, 10, 100, 1000] {
        let mb = *size;
        let data = TestData::generate_sstable(mb * 1024 * 1024);
        
        group.throughput(criterion::Throughput::Bytes(data.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}MB", mb)),
            &data,
            |b, data| b.iter(|| {
                SSTable::parse(black_box(data))
            })
        );
    }
    
    group.finish();
}

fn compression_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompression");
    
    for compression in &[CompressionType::LZ4, CompressionType::Snappy] {
        let compressed_data = TestData::compressed_block(*compression, 64 * 1024);
        
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{:?}", compression)),
            &compressed_data,
            |b, data| b.iter(|| {
                decompress(black_box(data), *compression)
            })
        );
    }
    
    group.finish();
}

criterion_group!(benches, parsing_benchmarks, compression_benchmarks);
criterion_main!(benches);
```

### **Memory Usage Testing**
```rust
#[test]
fn test_memory_usage() {
    use dhat::{Dhat, DhatAlloc};
    
    #[global_allocator]
    static ALLOCATOR: DhatAlloc = DhatAlloc;
    
    let _dhat = Dhat::start_heap_profiling();
    
    // Parse large file
    let large_file = TestData::load("stress/100mb-sstable.db");
    let sstable = SSTable::open(&large_file).unwrap();
    
    // Perform operations
    for i in 0..1000 {
        let key = format!("key{}", i);
        let _ = sstable.get_partition(key.as_bytes());
    }
    
    let stats = dhat::HeapStats::get();
    
    // Memory should be bounded
    assert!(stats.peak_bytes < 128 * 1024 * 1024); // <128MB for 100MB file
    assert!(stats.total_blocks < 10_000); // Reasonable allocation count
}
```

## 🌍 End-to-End Testing

### **Cassandra Compatibility Suite**
```rust
#[test]
fn e2e_cassandra_roundtrip() {
    // Start Cassandra cluster
    let cluster = CassandraCluster::start_three_node();
    
    // Create schema
    cluster.execute("CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
    cluster.execute("CREATE TABLE test.users (id UUID PRIMARY KEY, name TEXT, age INT)");
    
    // Insert data
    for i in 0..1000 {
        cluster.execute(&format!(
            "INSERT INTO test.users (id, name, age) VALUES ({}, 'User{}', {})",
            Uuid::new_v4(), i, i % 100
        ));
    }
    
    // Flush to create SSTable
    cluster.flush("test", "users");
    
    // Extract SSTable
    let sstable_path = cluster.get_sstable_path("test", "users");
    
    // Parse with CQLite
    let sstable = SSTable::open(&sstable_path).unwrap();
    assert_eq!(sstable.partition_count(), 1000);
    
    // Verify data integrity
    let mut count = 0;
    for partition in sstable.partitions() {
        count += 1;
        let row = partition.primary_row().unwrap();
        assert!(row.get_column("name").unwrap().as_text().starts_with("User"));
    }
    assert_eq!(count, 1000);
}
```

### **Cross-Version Testing**
```rust
#[test]
fn test_format_compatibility() {
    // Test against different Cassandra versions
    for version in &["4.1", "5.0", "5.1-beta"] {
        let cluster = CassandraCluster::start_version(version);
        let sstable_path = generate_test_sstable(&cluster);
        
        match SSTable::open(&sstable_path) {
            Ok(sstable) => {
                println!("Successfully parsed Cassandra {} format", version);
                assert!(sstable.partition_count() > 0);
            }
            Err(e) => {
                if version == "5.1-beta" {
                    // Expected - future version
                    assert!(matches!(e, ParseError::UnsupportedVersion(_)));
                } else {
                    panic!("Failed to parse Cassandra {} format: {}", version, e);
                }
            }
        }
    }
}
```

## 🔍 Fuzz Testing

### **Parser Fuzzing**
```rust
#[cfg(fuzzing)]
pub fn fuzz_sstable_parser(data: &[u8]) {
    // Try to parse as SSTable
    let _ = SSTable::parse(data);
    
    // Try to parse components
    let _ = parse_header(data);
    let _ = parse_partition(data);
    let _ = parse_index_entry(data);
}

// Run with: cargo fuzz run sstable_parser
```

### **Differential Testing**
```rust
#[test]
fn differential_test_against_cassandra() {
    let test_data = TestData::load("differential/complex-data.db");
    
    // Parse with CQLite
    let cqlite_result = SSTable::open(&test_data).unwrap();
    
    // Parse with Cassandra's sstablescrub
    let cassandra_result = run_sstablescrub(&test_data);
    
    // Compare results
    assert_eq!(cqlite_result.partition_count(), cassandra_result.partitions);
    assert_eq!(cqlite_result.row_count(), cassandra_result.rows);
    assert_eq!(cqlite_result.tombstone_count(), cassandra_result.tombstones);
}
```

## 📊 Test Coverage Requirements

### **Coverage Targets**
- **Overall**: 90% line coverage
- **Core Parser**: 95% line coverage
- **Error Paths**: 85% coverage
- **FFI Boundary**: 100% coverage

### **Coverage Monitoring**
```bash
# Generate coverage report
cargo tarpaulin --out Html --output-dir coverage

# Check coverage thresholds
cargo tarpaulin --print-summary --fail-under 90
```

## 🐛 Regression Testing

### **Bug Reproduction Suite**
```rust
// Each fixed bug gets a regression test
#[test]
fn regression_issue_42_boundary_overflow() {
    // Bug: Overflow when parsing large vint
    let problematic_data = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F];
    
    match parse_vint(problematic_data) {
        Ok((_, value)) => assert_eq!(value, i64::MAX),
        Err(e) => panic!("Regression: vint parsing failed: {:?}", e),
    }
}
```

## 🔧 Testing Utilities

### **Test Helpers**
```rust
pub mod test_utils {
    /// Creates a minimal valid SSTable for testing
    pub fn minimal_sstable() -> Vec<u8> {
        SSTableBuilder::new()
            .format_version("oa")
            .add_partition("key1", vec![("col1", "value1")])
            .build()
    }
    
    /// Corrupts data in specific ways for error testing
    pub fn corrupt_checksum(data: &mut [u8]) {
        if data.len() >= 4 {
            data[data.len() - 4] ^= 0xFF;
        }
    }
    
    /// Generates deterministic test data
    pub fn deterministic_data(seed: u64, size: usize) -> Vec<u8> {
        use rand::{SeedableRng, RngCore};
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let mut data = vec![0u8; size];
        rng.fill_bytes(&mut data);
        data
    }
}
```

---

*This comprehensive testing strategy ensures CQLite maintains compatibility, performance, and reliability across all supported use cases and platforms.*