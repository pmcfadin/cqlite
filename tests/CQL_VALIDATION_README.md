# CQL Schema Validation Test Suite

This comprehensive test suite validates the CQL (Cassandra Query Language) schema parsing capabilities of CQLite. It ensures accurate parsing of CREATE TABLE statements, proper type conversions, error handling, and performance characteristics.

## 🎯 Test Suite Components

### 1. CQL Parser Validation Suite (`cql_parser_validation_suite.rs`)

**Purpose**: Validates basic CQL parsing functionality

**Features**:
- ✅ Basic CQL CREATE TABLE parsing
- ✅ CREATE TABLE format variations (IF NOT EXISTS, WITH options, etc.)
- ✅ Table name matching logic
- ✅ Type conversion validation (all CQL types)
- ✅ Complex type parsing (collections, UDTs, tuples)
- ✅ Error handling for malformed CQL
- ✅ JSON vs CQL schema output comparison
- ✅ Performance testing with large CQL files

**Test Categories**:
- `basic_cql_parsing`: Simple CREATE TABLE statements
- `create_table_variations`: Different CREATE TABLE syntaxes
- `table_name_matching`: Pattern matching for table identification
- `type_conversions`: CQL type to internal type mapping
- `complex_types`: Collections, UDTs, frozen types
- `error_handling`: Malformed CQL error detection
- `json_vs_cql_schemas`: Schema format consistency
- `parser_performance`: Performance with large schemas

### 2. CQL Integration Tests (`cql_integration_tests.rs`)

**Purpose**: End-to-end integration testing with real CQL files

**Features**:
- 🔗 Complete CQL-to-schema workflow testing
- 🔗 Schema validation pipeline integration
- 🔗 UDT (User Defined Type) integration
- 🔗 Complex real-world schema scenarios
- 🔗 Schema roundtrip testing (CQL → JSON → validation)
- 🔗 Error recovery and graceful degradation
- 🔗 Large schema performance testing

**Test Categories**:
- `cql_file_parsing`: Real CQL file processing
- `schema_validation_workflow`: Complete validation pipeline
- `udt_integration`: User Defined Type handling
- `complex_scenarios`: Real-world-like schemas
- `schema_roundtrip`: Format conversion consistency
- `error_recovery`: Graceful error handling
- `large_schema_performance`: Scalability testing

### 3. CQL Performance Benchmarks (`cql_performance_benchmarks.rs`)

**Purpose**: Performance characteristics and optimization validation

**Features**:
- ⚡ Throughput measurement (operations/second)
- ⚡ Latency analysis (average, min, max, p99)
- ⚡ Memory usage tracking
- ⚡ Concurrent parsing performance
- ⚡ Type conversion benchmarks
- ⚡ Stress testing with extreme workloads

**Benchmark Categories**:
- `basic_parsing`: Simple CQL statement parsing speed
- `complex_type_parsing`: Complex type parsing performance
- `large_schema_parsing`: Large schema handling efficiency
- `concurrent_parsing`: Multi-threaded parsing performance
- `memory_usage`: Memory consumption patterns
- `schema_validation`: Validation pipeline performance
- `type_conversions`: Type conversion speed
- `stress_testing`: Extreme workload handling

### 4. Test Data Fixtures (`cql_test_data_fixtures.rs`)

**Purpose**: Comprehensive test data for thorough validation

**Features**:
- 📊 CQL test cases with expected outcomes
- 📊 Type conversion test cases
- 📊 Error test cases for malformed CQL
- 📊 Performance test data generators
- 📊 Real-world schema examples
- 📊 Compatibility test fixtures

**Data Categories**:
- **Basic CQL Statements**: Simple CREATE TABLE variations
- **Complex CQL Statements**: Advanced features (collections, UDTs)
- **UDT Test Cases**: User Defined Type scenarios
- **Error Test Cases**: Malformed CQL examples
- **Type Conversion Cases**: All CQL type mappings
- **Performance Schemas**: Large-scale test data
- **Real-World Schemas**: Production-like examples
- **JSON Schema Fixtures**: Expected output formats

## 🚀 Running the Tests

### Quick Start

```bash
# Run all validation tests
./run_cql_validation_tests.sh

# Run with verbose output and HTML reports
./run_cql_validation_tests.sh --verbose --html
```

### Specific Test Suites

```bash
# Run only validation tests
./run_cql_validation_tests.sh --validation-only

# Run only integration tests
./run_cql_validation_tests.sh --integration-only

# Run only performance benchmarks
./run_cql_validation_tests.sh --benchmarks-only
```

### Using the Test Runner Directly

```bash
# Build and run the test runner
cargo build --release
cargo run --release --bin cql_validation_test_runner

# With specific options
cargo run --release --bin cql_validation_test_runner -- \
    --validation --integration --benchmarks \
    --verbose --html \
    --output target/reports \
    --timeout 600
```

## 📊 Test Reports

The test suite generates comprehensive reports in multiple formats:

### JSON Reports
- `validation_report.json`: Detailed validation test results
- `integration_report.json`: Integration test outcomes
- `benchmark_report.json`: Performance benchmark data
- `consolidated_report.json`: Summary of all test results

### HTML Report
- `test_report.html`: Interactive web-based report (with `--html` flag)

### Report Contents

Each report includes:
- ✅ **Test Results**: Pass/fail status for each test
- ⏱️ **Execution Times**: Performance metrics
- 📈 **Throughput Data**: Operations per second
- 💾 **Memory Usage**: Peak memory consumption
- 🔍 **Error Details**: Failure explanations and suggestions
- 📊 **Performance Trends**: Benchmark comparisons

## 🧪 Test Coverage

### CQL Features Tested

#### Basic Table Creation
- [x] Simple tables with primary keys
- [x] Composite primary keys
- [x] Clustering keys with ordering
- [x] Table options and metadata

#### Data Types
- [x] All primitive types (BOOLEAN, INT, BIGINT, etc.)
- [x] Date/time types (TIMESTAMP, DATE, TIME)
- [x] Network types (INET, UUID, TIMEUUID)
- [x] Binary types (BLOB, custom types)

#### Collection Types
- [x] LIST\<type\> collections
- [x] SET\<type\> collections  
- [x] MAP\<key, value\> collections
- [x] Nested collections (LIST\<SET\<type\>\>)
- [x] FROZEN collections

#### Complex Types
- [x] TUPLE types with multiple elements
- [x] User Defined Types (UDTs)
- [x] UDTs in collections
- [x] Nested UDTs

#### Advanced Features
- [x] IF NOT EXISTS clauses
- [x] WITH table options
- [x] CLUSTERING ORDER BY
- [x] Keyspace-qualified table names
- [x] Quoted identifiers

### Error Handling Tested
- [x] Missing semicolons
- [x] Invalid table names
- [x] Missing primary keys
- [x] Invalid data types
- [x] Unclosed parentheses
- [x] Empty collection types
- [x] Malformed primary key definitions
- [x] Reserved keyword usage
- [x] Invalid clustering orders

### Performance Scenarios
- [x] Small schemas (< 10 columns)
- [x] Medium schemas (10-50 columns) 
- [x] Large schemas (50-200 columns)
- [x] Extra large schemas (200+ columns)
- [x] Deeply nested collections
- [x] Concurrent parsing workloads
- [x] Memory usage patterns
- [x] Type conversion performance

## 🎯 Performance Targets

The benchmark suite validates these performance targets:

### Throughput Targets
- **Basic Parsing**: ≥ 1,000 ops/sec
- **Complex Types**: ≥ 100 ops/sec  
- **Large Schemas**: ≥ 10 ops/sec
- **Type Conversions**: ≥ 10,000 ops/sec

### Latency Targets
- **Basic Parsing**: ≤ 1ms average
- **Complex Types**: ≤ 10ms average
- **Large Schemas**: ≤ 100ms average
- **Type Conversions**: ≤ 0.1ms average

### Memory Targets
- **Basic Operations**: ≤ 1MB peak usage
- **Complex Types**: ≤ 2MB peak usage
- **Large Schemas**: ≤ 16MB peak usage
- **Stress Tests**: ≤ 32MB peak usage

## 🔧 Customization

### Adding New Test Cases

1. **CQL Test Cases**: Add to `PerformanceTestData` in `cql_test_data_fixtures.rs`
2. **Error Cases**: Add to `error_test_cases()` method
3. **Type Cases**: Add to `type_conversion_test_cases()` method
4. **Integration Scenarios**: Add to integration test workflow methods

### Adding New Benchmarks

1. Add benchmark method to `CqlPerformanceBenchmarkSuite`
2. Define performance targets in `PerformanceTargets`
3. Update `run_all_benchmarks()` to include new test
4. Add result reporting to benchmark report generation

### Extending Integration Tests

1. Add new test methods to `CqlIntegrationTestSuite`
2. Create test data in `create_comprehensive_cql_files()`
3. Add validation logic for new scenarios
4. Update report generation for new test categories

## 🐛 Troubleshooting

### Common Issues

**Build Errors:**
```bash
# Ensure all dependencies are installed
cargo check
cargo build --release
```

**Test Timeouts:**
```bash
# Increase timeout for large schemas
./run_cql_validation_tests.sh --timeout 600
```

**Memory Issues:**
```bash
# Run with smaller test sets
./run_cql_validation_tests.sh --validation-only
```

**Permission Errors:**
```bash
# Ensure output directory is writable
chmod 755 target/
mkdir -p target/cql_validation_reports
```

### Debug Mode

For detailed debugging information:

```bash
# Enable verbose output
./run_cql_validation_tests.sh --verbose

# Check specific test logs
tail -f target/cql_validation_reports/validation_report.json
```

## 📈 Continuous Integration

### GitHub Actions Integration

```yaml
name: CQL Validation Tests

on: [push, pull_request]

jobs:
  cql-validation:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
      - name: Run CQL Validation Tests
        run: ./run_cql_validation_tests.sh --html
      - name: Upload Reports
        uses: actions/upload-artifact@v2
        with:
          name: cql-validation-reports
          path: target/cql_validation_reports/
```

### Performance Regression Detection

The benchmark suite can detect performance regressions by comparing against baseline metrics:

```bash
# Run benchmarks and compare against baseline
./run_cql_validation_tests.sh --benchmarks-only
# Results are automatically compared against performance targets
```

## 🏆 Success Criteria

A successful validation run should show:

- ✅ All validation tests pass (100% success rate)
- ✅ All integration tests pass (100% success rate)  
- ✅ All benchmarks meet performance targets
- ✅ Memory usage within acceptable limits
- ✅ No performance regressions detected
- ✅ Error handling covers all malformed cases
- ✅ Type conversions work for all CQL types

## 🤝 Contributing

To add new validation tests:

1. **Fork the repository**
2. **Add test cases** to appropriate fixture files
3. **Implement test logic** in validation suite files
4. **Update performance targets** if needed
5. **Add documentation** for new test scenarios
6. **Run full validation suite** to ensure compatibility
7. **Submit pull request** with test results

---

**Note**: This validation suite is designed to ensure CQLite maintains 100% compatibility with Cassandra 5+ CQL schema parsing. All tests should pass for production readiness.