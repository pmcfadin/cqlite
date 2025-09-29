# CQLite Property-Based Testing Framework

This crate provides comprehensive property-based testing for CQLite's type system, compression algorithms, and edge case handling using the [proptest](https://crates.io/crates/proptest) framework.

## Overview

Property-based testing validates that certain properties hold true across a wide range of generated inputs, helping catch edge cases that traditional unit tests might miss.

## Features

### 🎯 Core Properties Tested

1. **All CQL Types Roundtrip Serialization/Deserialization**
   - Validates that every CQL value can be serialized and deserialized identically
   - Special handling for floating-point NaN and infinity values
   - Supports all CQL types: primitives, collections, UDTs, frozen types

2. **Schema Inference Consistency**
   - Ensures schema definitions are consistent across serialization formats
   - Validates schema invariants (non-empty names, unique columns, etc.)
   - Tests schema component size limits

3. **Compression Data Integrity**
   - Tests all compression algorithms: LZ4, Snappy, Deflate, Zstd
   - Validates data integrity after compression/decompression roundtrips
   - Checks compression ratio bounds for different data patterns

4. **Partition Key Handling**
   - Tests edge cases in partition and clustering key serialization
   - Validates key uniqueness and ordering properties
   - Ensures keys maintain consistency across operations

5. **Memory Usage Bounds**
   - Prevents memory leaks in serialization operations
   - Validates memory usage patterns stay within bounds
   - Tests concurrent memory access patterns

6. **Performance Regression Prevention**
   - Ensures operations complete within time bounds
   - Validates that performance doesn't degrade unexpectedly
   - Tests concurrent operation throughput

## Architecture

### Type System (`src/types.rs`)

```rust
pub enum CqlValue {
    Null,
    Boolean(bool),
    Integer(i32),
    BigInt(i64),
    Float(OrderedFloat),    // Hashable wrapper for f64
    Text(String),
    Blob(Vec<u8>),
    // ... and many more
}
```

The `CqlValue` enum mirrors `cqlite_core::types::Value` but is designed specifically for property testing with additional traits like `Hash` and `Eq`.

### Generators (`src/generators.rs`)

Proptest generators create arbitrary values for testing:

```rust
// Generate any CQL value
fn arb_cql_value() -> impl Strategy<Value = CqlValue>

// Generate extreme numeric values (boundaries, NaN, infinity)
fn arb_extreme_numerics() -> impl Strategy<Value = CqlValue>

// Generate deeply nested structures
fn arb_deeply_nested(max_depth: usize) -> impl Strategy<Value = CqlValue>

// Generate schema definitions
fn arb_schema() -> impl Strategy<Value = Schema>
```

### Compression Testing (`src/compression.rs`)

Mock compression algorithms for testing compression properties:

```rust
pub enum CompressionType {
    None,
    Lz4Mock,     // Fast compression with modest ratios
    SnappyMock,  // Balanced speed/compression
    DeflateMock, // Better compression, slower
    ZstdMock,    // Hybrid approach
}
```

### Validation (`src/validation.rs`)

Validation functions ensure generated values meet invariants:

```rust
// Validate CQL value constraints
validate_cql_value_invariants(value: &CqlValue)

// Validate schema structure
validate_schema_invariants(schema: &Schema)

// Validate performance bounds
validate_performance_bounds(duration, max_duration, operation)
```

## Running Tests

### Basic Test Execution

```bash
# Run all property tests
cargo test

# Run with more test cases (default is 256)
PROPTEST_CASES=1000 cargo test

# Run specific test
cargo test prop_all_cql_types_roundtrip

# Run with verbose output
cargo test -- --nocapture
```

### Performance Testing

```bash
# Enable performance tests
cargo test --features performance-tests

# Stress testing with large inputs
cargo test --features stress-tests
```

### Configuration

Property tests can be configured via environment variables:

```bash
# Number of test cases to generate
export PROPTEST_CASES=1000

# Maximum test case size
export PROPTEST_MAX_SHRINK_ITERS=10000

# Timeout per test case
export PROPTEST_TIMEOUT=30000
```

## Test Categories

### 1. Roundtrip Tests

Validate that values survive serialization/deserialization:

```rust
proptest! {
    #[test]
    fn prop_all_cql_types_roundtrip(value in arb_cql_value()) {
        let serialized = bincode::serialize(&value)?;
        let deserialized: CqlValue = bincode::deserialize(&serialized)?;

        // Handle NaN equality specially
        match (&value, &deserialized) {
            (CqlValue::Float(f1), CqlValue::Float(f2)) => {
                validate_float_special_values(f1.0, f2.0)?;
            },
            _ => prop_assert_eq!(value, deserialized),
        }
    }
}
```

### 2. Edge Case Tests

Test boundary conditions and extreme values:

```rust
proptest! {
    #[test]
    fn prop_edge_cases(value in arb_extreme_numerics()) {
        // Test i32::MIN, i32::MAX, f64::NAN, f64::INFINITY, etc.
        let serialized = bincode::serialize(&value)?;
        let deserialized: CqlValue = bincode::deserialize(&serialized)?;
        // Validate special value handling
    }
}
```

### 3. Compression Tests

Validate compression algorithm properties:

```rust
proptest! {
    #[test]
    fn prop_compression_data_integrity(
        data in arb_compression_data(),
        algorithm in arb_compression_type()
    ) {
        let codec = CompressionCodec::new(algorithm)?;
        let compressed = codec.compress(&data)?;
        let decompressed = codec.decompress(&compressed, data.len())?;

        prop_assert_eq!(data, decompressed);
        validate_compression_ratio(/* ... */)?;
    }
}
```

### 4. Concurrent Safety Tests

Ensure thread safety:

```rust
proptest! {
    #[test]
    fn prop_concurrent_safety(values in arb_values()) {
        let handles: Vec<_> = values.iter().map(|value| {
            thread::spawn(|| {
                // Perform operations concurrently
                let serialized = bincode::serialize(value)?;
                bincode::deserialize(&serialized)
            })
        }).collect();

        // Validate all results are correct
    }
}
```

## Edge Cases Covered

### Numeric Edge Cases
- Integer boundaries: `i32::MIN`, `i32::MAX`, `i64::MIN`, `i64::MAX`
- Floating point special values: `NaN`, `INFINITY`, `NEG_INFINITY`, `±0.0`
- Precision boundaries for decimals and varints

### Text Edge Cases
- Empty strings
- Unicode edge cases: 2-byte, 3-byte, 4-byte UTF-8 sequences
- Very long strings (up to 1MB)
- Control characters and special symbols

### Binary Edge Cases
- Empty blobs
- Large binary data (up to 1MB)
- Repetitive patterns (highly compressible)
- Random data (incompressible)
- Common binary file headers

### Collection Edge Cases
- Empty collections
- Very large collections (memory pressure testing)
- Deeply nested structures (recursion limits)
- Collections with duplicate values
- Mixed type collections

### Schema Edge Cases
- Minimum/maximum column counts
- Very long identifiers
- All supported CQL data types
- Complex nested type definitions

## Performance Characteristics

### Benchmarks

Property tests include performance validation:

- **Serialization**: < 1 second per value
- **Deserialization**: < 1 second per value
- **Compression**: Varies by algorithm and data size
- **Memory Usage**: Bounded relative to data size

### Compression Ratios

Expected compression ratios for different algorithms:

| Algorithm | Repetitive Data | Random Data | Structured Data |
|-----------|----------------|-------------|-----------------|
| LZ4Mock   | 0.05-0.1       | 0.9-1.1     | 0.3-0.7        |
| SnappyMock| 0.05-0.1       | 0.8-1.1     | 0.3-0.6        |
| DeflateMock| 0.01-0.05     | 0.6-1.1     | 0.2-0.5        |
| ZstdMock  | 0.01-0.05      | 0.7-1.1     | 0.2-0.6        |

## Integration

### With Main Codebase

This property testing framework is designed to complement the main `cqlite-core` library:

1. **Type Compatibility**: `CqlValue` mirrors `cqlite_core::types::Value`
2. **Serialization**: Uses same `bincode` format as main codebase
3. **Validation**: Tests same invariants as production code

### CI Integration

Add to CI pipeline:

```yaml
- name: Run Property Tests
  run: |
    cd property-tests
    PROPTEST_CASES=1000 cargo test --all-features
    cargo test --doc
```

## Extending Tests

### Adding New CQL Types

1. Add variant to `CqlValue` enum in `src/types.rs`
2. Add generator in `src/generators.rs`
3. Add validation in `src/validation.rs`
4. Update property tests

### Adding New Properties

```rust
proptest! {
    #[test]
    fn prop_new_property(input in your_generator()) {
        // Test your property
        prop_assert!(your_property_holds(&input));
    }
}
```

### Custom Generators

```rust
fn arb_your_type() -> impl Strategy<Value = YourType> {
    // Use proptest combinators
    (arb_field1(), arb_field2()).prop_map(|(f1, f2)| YourType { f1, f2 })
}
```

## Troubleshooting

### Common Issues

1. **Test Timeouts**: Reduce `PROPTEST_CASES` or increase timeout
2. **Memory Issues**: Use smaller collection sizes in generators
3. **Shrinking Issues**: Increase `PROPTEST_MAX_SHRINK_ITERS`

### Debugging Failing Tests

```bash
# Get detailed failure information
PROPTEST_VERBOSE=1 cargo test failing_test_name

# Reproduce specific failure
PROPTEST_PERSIST_FILE=.proptest-regressions/file cargo test
```

## Future Enhancements

1. **More Compression Algorithms**: Add real implementations
2. **Database Integration**: Test with actual SSTable files
3. **Network Serialization**: Test protocol buffer compatibility
4. **Fuzzing Integration**: Connect with fuzzing frameworks
5. **Performance Benchmarking**: Automated performance regression detection

## Contributing

1. Follow existing patterns in generators and validation
2. Add comprehensive documentation for new properties
3. Include both positive and negative test cases
4. Consider edge cases specific to your domain
5. Update this README with new test categories

## Resources

- [Proptest Documentation](https://docs.rs/proptest/)
- [Property-Based Testing Guide](https://hypothesis.works/articles/what-is-property-based-testing/)
- [CQLite Main Repository](../README.md)