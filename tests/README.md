# CQLite Cassandra 5+ Compatibility Test Suite

A comprehensive test suite for validating CQLite's compatibility with Cassandra 5+ SSTable format and functionality.

## 🎯 Overview

This test suite ensures that CQLite can correctly read, parse, and process Cassandra 5+ data files with full format compatibility. It includes:

- **SSTable Format Validation**: Header parsing, compression, metadata handling
- **CQL Type System Tests**: All primitive, collection, and special data types
- **Performance Benchmarks**: Throughput, latency, and memory usage analysis
- **Edge Case Testing**: Error handling, malformed data, boundary conditions
- **Integration Testing**: End-to-end compatibility validation

## 🚀 Quick Start

### Run Full Compatibility Test Suite

```bash
# From the tests directory
cargo run --bin compatibility_test_runner

# Or from project root
cargo run --manifest-path tests/Cargo.toml --bin compatibility_test_runner
```

### Run Quick Compatibility Check

```bash
cargo run --bin compatibility_test_runner -- --mode quick
```

### Run Performance Benchmarks

```bash
cargo run --bin compatibility_test_runner -- --mode performance --stress
```

## 📋 Test Categories

### 1. SSTable Format Tests (`sstable_format_tests.rs`)

Tests Cassandra 5+ SSTable binary format compatibility:

- **Header Parsing**: Magic numbers, version validation, metadata extraction
- **Compression Support**: LZ4, Snappy, and uncompressed formats
- **Statistics Parsing**: Row counts, timestamps, compression ratios
- **Column Information**: Primary keys, clustering columns, static columns
- **Properties**: Table-level configuration and metadata
- **Error Handling**: Malformed headers, version mismatches, truncated data

**Key Features:**
- Validates round-trip serialization/deserialization
- Tests large headers with many columns and properties
- Ensures proper error handling for corrupted data

### 2. CQL Type System Tests (`type_system_tests.rs`)

Comprehensive validation of all CQL data types:

**Primitive Types:**
- `BOOLEAN`, `TINYINT`, `SMALLINT`, `INT`, `BIGINT`
- `FLOAT`, `DOUBLE`, `DECIMAL`, `VARINT`
- `TEXT`, `ASCII`, `BLOB`

**Temporal Types:**
- `TIMESTAMP`, `DATE`, `TIME`, `DURATION`

**Special Types:**
- `UUID`, `TIMEUUID`, `INET`

**Collection Types:**
- `LIST<T>`, `SET<T>`, `MAP<K,V>`

**Advanced Features:**
- Unicode string handling
- Large data values (>1MB)
- Null value processing
- Type conversion compatibility
- Edge cases and boundary values

### 3. Performance Benchmarks (`performance_benchmarks.rs`)

Measures performance across key operations:

**Parsing Performance:**
- Header parsing throughput (ops/sec)
- Value serialization/deserialization speed
- Collection handling performance

**Storage Operations:**
- Write throughput and latency
- Read performance and caching
- Concurrent operation handling

**Scalability Testing:**
- Large dataset processing (100K+ records)
- Memory efficiency analysis
- Compression performance

**Stress Testing:**
- High-volume operations (1M+ ops)
- Concurrent load testing
- Resource usage monitoring

### 4. Compatibility Framework (`compatibility_framework.rs`)

End-to-end compatibility validation:

- **Real Data Simulation**: Creates mock Cassandra 5+ data structures
- **Format Validation**: Ensures binary compatibility with Cassandra
- **Error Detection**: Identifies compatibility gaps and issues
- **Scoring System**: Provides quantitative compatibility assessment
- **Detailed Reporting**: Comprehensive test results and recommendations

### 5. Integration Runner (`integration_runner.rs`)

Orchestrates all test categories:

- **Unified Interface**: Single command to run all tests
- **Configurable Execution**: Select specific test categories
- **Comprehensive Reporting**: Detailed results and scoring
- **CI/CD Integration**: Exit codes and structured output

## 🛠️ Usage Examples

### Custom Test Selection

```bash
# Run only format and type tests
cargo run --bin compatibility_test_runner -- --format-tests --type-tests

# Run compatibility tests with detailed reporting
cargo run --bin compatibility_test_runner -- --compatibility-tests --detailed

# Run performance tests with stress testing
cargo run --bin compatibility_test_runner -- --performance-tests --stress
```

### Advanced Options

```bash
# Fail fast on first error
cargo run --bin compatibility_test_runner -- --fail-fast

# Enable all options for comprehensive testing
cargo run --bin compatibility_test_runner -- --stress --detailed --fail-fast
```

## 📊 Test Results and Scoring

The test suite provides comprehensive scoring across multiple dimensions:

### Compatibility Score (0.0 - 1.0)
- **1.0**: Perfect compatibility, production-ready
- **0.9+**: Excellent compatibility, minor issues only
- **0.8+**: Good compatibility, some gaps present
- **0.7+**: Acceptable compatibility, needs improvement
- **<0.7**: Significant compatibility issues

### Performance Score (0.0 - 1.0)
- Based on throughput benchmarks
- Considers latency percentiles (P95, P99)
- Evaluates memory efficiency
- Assesses concurrent operation handling

### Overall Assessment
- **🟢 Production Ready**: >95% overall score
- **🟡 Mostly Compatible**: 85-95% overall score
- **🟠 Needs Improvement**: 70-85% overall score
- **🔴 Significant Issues**: <70% overall score

## 🔧 Test Configuration

### Environment Variables

```bash
# Enable verbose logging
export RUST_LOG=debug

# Customize test data size
export CQLITE_TEST_DATASET_SIZE=100000

# Enable stress testing by default
export CQLITE_ENABLE_STRESS_TESTS=true
```

### Configuration Files

Test behavior can be customized through code configuration:

```rust
use integration_tests::{IntegrationTestConfig, IntegrationTestRunner};

let config = IntegrationTestConfig {
    run_compatibility_tests: true,
    run_format_tests: true,
    run_type_tests: true,
    run_performance_benchmarks: true,
    run_stress_tests: false,  // Disable for faster testing
    detailed_reporting: true,
    fail_fast: false,
};

let runner = IntegrationTestRunner::new(config);
let results = runner.run_all_tests().await?;
```

## 🧪 Adding New Tests

### Adding SSTable Format Tests

```rust
// In sstable_format_tests.rs
impl SSTableFormatTests {
    fn test_new_feature(&self) -> Result<()> {
        // Test implementation
        println!("  Testing new feature...");
        // ... test logic
        println!("    ✓ New feature test passed");
        Ok(())
    }
}
```

### Adding Type System Tests

```rust
// In type_system_tests.rs
impl TypeSystemTests {
    fn test_new_type(&mut self) -> Result<()> {
        let test_value = Value::NewType(/* ... */);
        self.test_type_roundtrip("NEW_TYPE", CqlTypeId::NewType, test_value)?;
        Ok(())
    }
}
```

### Adding Performance Benchmarks

```rust
// In performance_benchmarks.rs
impl PerformanceBenchmarks {
    async fn benchmark_new_operation(&mut self) -> Result<()> {
        let start = Instant::now();
        // ... benchmark logic
        let elapsed = start.elapsed();
        
        self.results.push(BenchmarkResult {
            test_name: "New Operation".to_string(),
            operations_per_second: ops_per_sec,
            // ... other metrics
        });
        
        Ok(())
    }
}
```

## 🔍 Debugging Test Failures

### Enable Detailed Logging

```bash
RUST_LOG=debug cargo run --bin compatibility_test_runner -- --detailed
```

### Run Specific Test Categories

```bash
# Test only type system if format tests are failing
cargo run --bin compatibility_test_runner -- --type-tests

# Isolate performance issues
cargo run --bin compatibility_test_runner -- --performance-tests
```

### Analyze Test Output

The test runner provides detailed output for debugging:

```
❌ Failed Tests:
  • LARGE_LIST: Value mismatch: expected List(1000 items), got List(999 items)
  • UNICODE: Parse error: InvalidUtf8Sequence

📊 Performance Issues:
  • Header parsing: 2,450 ops/sec (below 5,000 threshold)
  • Memory usage: 156.7 MB (high usage detected)
```

## 🚀 Continuous Integration

### GitHub Actions Integration

```yaml
- name: Run Compatibility Tests
  run: |
    cd tests
    cargo run --bin compatibility_test_runner -- --fail-fast
    
- name: Run Performance Benchmarks
  run: |
    cd tests
    cargo run --bin compatibility_test_runner -- --mode performance
```

### Exit Codes

- `0`: All tests passed, good compatibility
- `1`: Some tests failed
- `2`: Low compatibility score (<85%)
- `3`: Test execution error

## 📚 Technical Details

### Test Data Generation

The test suite generates realistic Cassandra 5+ data:

- **Mock SSTable Files**: Binary-compatible headers and data blocks
- **Realistic Data Sets**: User tables, order data, time-series data
- **Edge Cases**: Empty values, maximum sizes, Unicode content
- **Error Conditions**: Corrupted data, invalid formats

### Binary Format Validation

Tests ensure bit-perfect compatibility with Cassandra 5+ format:

- **Magic Numbers**: `0x6F610000` ('oa' format identifier)
- **VInt Encoding**: Variable-length integer encoding/decoding
- **String Serialization**: Length-prefixed UTF-8 strings
- **Collection Formats**: Type-specific serialization for lists, sets, maps

### Performance Methodology

Benchmarks use statistical analysis for accurate measurements:

- **Warm-up Runs**: Eliminate JIT compilation effects
- **Multiple Iterations**: Statistical significance
- **Percentile Analysis**: P95, P99 latency measurements
- **Memory Profiling**: Peak and average usage tracking

## 🤝 Contributing

When adding new tests:

1. **Follow Naming Conventions**: Use descriptive test names
2. **Add Documentation**: Explain what each test validates
3. **Include Edge Cases**: Test boundary conditions
4. **Provide Examples**: Show expected vs. actual results
5. **Update This README**: Document new test categories

## 📄 License

This test suite is part of the CQLite project and follows the same license terms.

---

**🎯 Goal**: Ensure 100% compatibility with Cassandra 5+ SSTable format for seamless data migration and interoperability.