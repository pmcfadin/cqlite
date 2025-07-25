# CQLite CLI Testing Guide

This guide documents the comprehensive testing strategy and procedures for the CQLite CLI.

## 🧪 Test Suite Overview

The CQLite CLI test suite is designed to ensure reliability, performance, and correctness across all functionality:

### Test Categories

1. **Unit Tests** (`tests/unit_tests.rs`)
   - Configuration loading and validation
   - Command-line argument parsing
   - Output formatting logic
   - Error handling functions
   - Utility function validation

2. **Integration Tests** (`tests/integration_tests.rs`)
   - CLI command execution
   - Database operations
   - File I/O operations
   - Cross-module interactions
   - Output format validation

3. **End-to-End Tests** (`tests/end_to_end_tests.rs`)
   - Complete user workflows
   - Real-world usage scenarios
   - Performance under load
   - Cross-platform compatibility
   - Memory usage patterns

4. **Error Handling Tests** (`tests/error_handling_tests.rs`)
   - Invalid input validation
   - Resource constraint handling
   - Security vulnerability prevention
   - Graceful failure scenarios
   - Recovery mechanisms

5. **Test Infrastructure** (`tests/test_helpers.rs`)
   - Common test utilities
   - Test data generation
   - Environment setup/teardown
   - Performance measurement
   - Result validation

## 🚀 Running Tests

### Quick Test Execution

```bash
# Run all tests
cargo test

# Run specific test category
cargo test unit_tests
cargo test integration_tests
cargo test end_to_end_tests
cargo test error_handling_tests

# Run with output
cargo test -- --nocapture

# Run ignored tests (performance/stress tests)
cargo test -- --ignored
```

### Environment-Controlled Testing

Set environment variables to control test execution:

```bash
# Enable specific test suites
export RUN_INTEGRATION_TESTS=1
export RUN_UNIT_TESTS=1
export RUN_E2E_TESTS=1
export RUN_PERFORMANCE_TESTS=1
export RUN_ERROR_TESTS=1

# Test configuration
export VERBOSE=1
export TEST_TIMEOUT=60
export PARALLEL_TESTS=1

# Run tests with configuration
cargo test
```

### Using the Test Runner

```bash
# Use the comprehensive test runner
cargo run --bin test_runner

# Or run specific test patterns
cargo test test_cli_help
cargo test test_output_formats
cargo test test_sstable_operations
```

## 📋 Test Coverage Areas

### Command-Line Interface Testing

- ✅ Argument parsing validation
- ✅ Help and version display
- ✅ Global option handling
- ✅ Subcommand validation
- ✅ Flag combination testing
- ✅ Invalid argument rejection

### Database Operations Testing

- ✅ Database initialization
- ✅ Query execution
- ✅ Transaction handling
- ✅ Connection management
- ✅ Error recovery
- ✅ Performance monitoring

### Output Format Testing

- ✅ Table format validation
- ✅ JSON output correctness
- ✅ CSV format compliance
- ✅ YAML structure validation
- ✅ Format switching
- ✅ Large data handling

### Schema Management Testing

- ✅ JSON schema validation
- ✅ CQL DDL parsing
- ✅ Schema file auto-detection
- ✅ Validation error reporting
- ✅ Schema creation/modification
- ✅ Cross-format compatibility

### SSTable Operations Testing

- ✅ Directory structure validation
- ✅ File format detection
- ✅ Version compatibility
- ✅ Data extraction
- ✅ Statistics analysis
- ✅ Error condition handling

### Import/Export Testing

- ✅ Data format support
- ✅ File I/O operations
- ✅ Large dataset handling
- ✅ Error recovery
- ✅ Progress reporting
- ✅ Data integrity validation

### Performance Testing

- ✅ Benchmark execution
- ✅ Memory usage monitoring
- ✅ Concurrent operation handling
- ✅ Large dataset processing
- ✅ Resource constraint testing
- ✅ Performance regression detection

### Error Handling Testing

- ✅ Invalid input graceful handling
- ✅ File access error management
- ✅ Network failure recovery
- ✅ Memory pressure scenarios
- ✅ Security vulnerability prevention
- ✅ User-friendly error messages

## 🛠 Test Infrastructure

### Test Helpers and Utilities

The `test_helpers.rs` module provides:

- **CLI Execution**: Safe command execution with timeout handling
- **Environment Setup**: Temporary directories, databases, and configurations
- **Data Generation**: Test schemas, datasets, and SSTable structures
- **Validation**: Output format checking and result verification
- **Performance**: Timing and resource usage measurement

### Test Data Management

- Temporary directories for isolated testing
- Mock SSTable structures for format testing
- Sample schema files (JSON and CQL)
- Generated datasets of various sizes
- Configuration files for different scenarios

### Assertions and Validation

```rust
// Command execution validation
assert!(command_succeeded(&output));
assert!(command_failed(&output));

// Output content validation
assert!(output_contains_all(&output, &["pattern1", "pattern2"]));
assert!(validate_output_format(&output, "json"));

// Performance validation
let timing = extract_timing_ms(&output);
assert!(timing.unwrap() < 1000.0); // Less than 1 second
```

## 🔍 Test Scenarios

### Basic Functionality Tests

1. **CLI Help and Version**
   ```bash
   cqlite --help
   cqlite --version
   ```

2. **Database Operations**
   ```bash
   cqlite --database test.db query "SELECT 1"
   cqlite --database test.db admin info
   ```

3. **Output Formats**
   ```bash
   cqlite --format json query "SELECT 1"
   cqlite --format csv query "SELECT 1"
   ```

### Advanced Workflow Tests

1. **Schema Management**
   ```bash
   cqlite schema validate schema.json
   cqlite schema create schema.cql
   cqlite schema list
   ```

2. **SSTable Analysis**
   ```bash
   cqlite info /path/to/sstable/
   cqlite read /path/to/sstable/ --schema schema.json
   ```

3. **Performance Benchmarks**
   ```bash
   cqlite bench read --ops 1000 --threads 4
   cqlite bench write --ops 500 --threads 2
   ```

### Error Condition Tests

1. **Invalid Arguments**
   ```bash
   cqlite --invalid-flag
   cqlite invalid-command
   ```

2. **File Access Errors**
   ```bash
   cqlite --database /nonexistent/path query "SELECT 1"
   cqlite info /invalid/sstable/path
   ```

3. **Malformed Input**
   ```bash
   cqlite query "INVALID SQL SYNTAX"
   cqlite schema validate invalid.json
   ```

## 📊 Performance Testing

### Benchmark Categories

1. **Query Performance**
   - Simple SELECT queries
   - Complex JOIN operations
   - Aggregation functions
   - Large result sets

2. **Data Processing**
   - Large file imports
   - Bulk data exports
   - SSTable reading performance
   - Memory usage under load

3. **Concurrent Operations**
   - Multiple simultaneous connections
   - Parallel query execution
   - Resource contention handling
   - Scalability limits

### Performance Metrics

- **Latency**: Query execution time
- **Throughput**: Operations per second
- **Memory**: Peak and average usage
- **CPU**: Processing efficiency
- **I/O**: Disk read/write performance

## 🚨 Known Limitations

### Current Status

⚠️ **Compilation Issues**: Some tests are limited due to current compilation errors in the CLI. Once these are resolved, full test functionality will be available.

### Test Dependencies

- **CLI Compilation**: Tests require successful compilation of the `cqlite` binary
- **Test Data**: Some tests require specific SSTable formats or schema files
- **Platform**: Some tests are Unix-specific (file permissions, etc.)
- **Resources**: Performance tests may require significant memory/CPU

## 🔧 Continuous Integration

### CI/CD Integration

```yaml
# Example GitHub Actions workflow
name: CLI Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - name: Run CLI tests
        run: |
          export RUN_INTEGRATION_TESTS=1
          export RUN_UNIT_TESTS=1
          export RUN_ERROR_TESTS=1
          cargo test --package cqlite-cli
```

### Test Automation

- **Pre-commit Hooks**: Run quick tests before commits
- **Pull Request Validation**: Full test suite on PR creation
- **Nightly Builds**: Extended performance and stress testing
- **Release Validation**: Comprehensive testing before releases

## 📈 Test Metrics and Reporting

### Coverage Tracking

- **Statement Coverage**: Line-by-line execution tracking
- **Branch Coverage**: Decision point validation
- **Function Coverage**: API endpoint testing
- **Integration Coverage**: Cross-module interaction validation

### Quality Gates

- **Minimum Coverage**: 80% statement coverage required
- **Performance Regression**: No more than 10% performance degradation
- **Security Validation**: All security tests must pass
- **Platform Compatibility**: Tests must pass on target platforms

## 🎯 Future Enhancements

### Planned Improvements

1. **Test Automation**
   - Automated test data generation
   - Property-based testing with QuickCheck
   - Mutation testing for robustness validation

2. **Performance Testing**
   - Benchmark regression tracking
   - Load testing with realistic workloads
   - Memory leak detection

3. **Security Testing**
   - Fuzzing for input validation
   - Security vulnerability scanning
   - Permission and access control testing

4. **User Experience Testing**
   - Interactive mode testing
   - Help text validation
   - Error message clarity assessment

## 🤝 Contributing to Tests

### Adding New Tests

1. **Choose the Appropriate Category**: Unit, integration, E2E, or error handling
2. **Follow Naming Conventions**: `test_feature_scenario`
3. **Use Test Helpers**: Leverage existing utilities for consistency
4. **Document Test Purpose**: Clear descriptions of what is being tested
5. **Handle Edge Cases**: Consider failure scenarios and boundary conditions

### Test Development Guidelines

- **Isolation**: Tests should not depend on external state
- **Repeatability**: Tests should produce consistent results
- **Performance**: Tests should execute efficiently
- **Maintainability**: Tests should be easy to understand and modify
- **Coverage**: Tests should validate both success and failure paths

### Code Review Checklist

- [ ] Test covers the intended functionality
- [ ] Test handles error conditions appropriately
- [ ] Test is properly isolated and doesn't affect other tests
- [ ] Test has clear and descriptive assertions
- [ ] Test documentation explains the purpose and expectations
- [ ] Test follows project coding standards
- [ ] Test execution time is reasonable

---

This testing guide ensures comprehensive validation of the CQLite CLI functionality and provides a foundation for reliable, high-quality software delivery.