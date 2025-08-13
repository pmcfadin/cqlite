# CQLite CLI Testing Guide

This document provides comprehensive guidance for testing the CQLite CLI, including the testing infrastructure, patterns, and best practices.

## Table of Contents

- [Overview](#overview)
- [Testing Infrastructure](#testing-infrastructure)
- [Test Organization](#test-organization)
- [Writing Tests](#writing-tests)
- [Running Tests](#running-tests)
- [Performance Testing](#performance-testing)
- [CI/CD Integration](#cicd-integration)
- [Best Practices](#best-practices)

## Overview

The CQLite CLI testing infrastructure is built around the **TestContainer pattern** for dependency injection and provides comprehensive utilities for:

- **Unit Testing**: Testing individual functions and modules
- **Integration Testing**: Testing CLI commands end-to-end
- **Performance Testing**: Benchmarking and load testing
- **Error Handling**: Validating error conditions and recovery
- **Fixture Management**: Creating and managing test data

## Testing Infrastructure

### TestContainer Pattern

The `TestContainer` provides isolated test environments with automatic cleanup:

```rust
use cqlite_cli::test_infrastructure::*;

#[tokio::test]
async fn test_cli_functionality() -> TestResult<()> {
    // Create isolated test environment
    test_container!(container);
    
    // Initialize CLI runner
    let runner = CliTestRunner::new(container);
    
    // Run tests
    runner.test_help()?;
    
    Ok(())
}
```

### Key Components

#### 1. TestContainer
- **Purpose**: Manages test lifecycle and dependencies
- **Features**: Automatic cleanup, isolated environments, configuration management
- **Usage**: `TestContainer::new()` or `test_container!(name)` macro

#### 2. CliTestRunner
- **Purpose**: Executes and validates CLI commands
- **Features**: Command building, assertion helpers, output validation
- **Usage**: Run commands and validate results

#### 3. TestDataBuilder
- **Purpose**: Creates test fixtures and sample data
- **Features**: SSTable generation, schema creation, data fixtures
- **Usage**: Build structured test data for realistic testing

#### 4. Assertion Helpers
- **Purpose**: Validate command results and outputs
- **Features**: JSON validation, table parsing, error checking
- **Usage**: Chain assertions for comprehensive validation

## Test Organization

### Directory Structure

```
cqlite-cli/
├── src/
│   ├── test_infrastructure/     # Testing utilities
│   │   ├── mod.rs              # Main module exports
│   │   ├── container.rs        # TestContainer implementation
│   │   ├── cli_helpers.rs      # CLI testing utilities
│   │   ├── fixtures.rs         # Test data builders
│   │   ├── assertions.rs       # Assertion helpers
│   │   ├── integration.rs      # Integration test framework
│   │   └── performance.rs      # Performance testing
│   └── lib.rs                  # Public API exports
├── tests/
│   └── integration/            # Integration tests
│       ├── mod.rs              # Test module organization
│       ├── cli_basic_tests.rs  # Basic CLI functionality
│       ├── cli_command_tests.rs # Command-specific tests
│       ├── database_operation_tests.rs # Database tests
│       ├── performance_tests.rs # Performance tests
│       └── error_handling_tests.rs # Error condition tests
└── docs/
    └── TESTING.md              # This document
```

### Test Categories

1. **Unit Tests** (`src/` with `#[cfg(test)]`)
   - Individual function testing
   - Module-level validation
   - Mock dependencies

2. **Integration Tests** (`tests/integration/`)
   - CLI command testing
   - End-to-end workflows
   - Database operations

3. **Performance Tests** (`tests/integration/performance_tests.rs`)
   - Benchmarking
   - Load testing
   - Resource usage validation

## Writing Tests

### Basic CLI Test

```rust
#[tokio::test]
async fn test_help_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.run(&["--help"])?
        .assert_success()?
        .stdout_contains("CQLite - High-performance embedded database")?;
    
    Ok(())
}
```

### Test with Fixtures

```rust
#[tokio::test]
async fn test_read_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    let env = container.environment();
    
    // Create test fixtures
    let fixtures = TestDataBuilder::new(env.fixtures_dir)?
        .add_sstable("users", 
            SSTableFixture::new("users")
                .add_column("id", "UUID")
                .add_column("name", "TEXT")
                .add_row(vec![
                    serde_json::Value::String("test-id".to_string()),
                    serde_json::Value::String("John Doe".to_string()),
                ])
        )?
        .add_schema("schema",
            SchemaFixture::new("test_keyspace")
                .add_table(
                    TableSchema::new("users")
                        .add_column("id", "UUID", false)
                        .add_column("name", "TEXT", false)
                        .with_primary_key(vec!["id".to_string()])
                )?
        )?
        .build()?;
    
    let sstable_path = &fixtures[0].file_path;
    let schema_path = &fixtures[1].file_path;
    
    runner.run(&[
        "read",
        sstable_path.to_str().unwrap(),
        "--schema",
        schema_path.to_str().unwrap(),
    ])?.assert_success()?;
    
    Ok(())
}
```

### Performance Test

```rust
#[tokio::test]
async fn test_command_performance() -> TestResult<()> {
    let runner = PerformanceTestRunner::new().await?
        .add_benchmark(
            PerformanceBenchmark::simple("help", "--help")
                .with_expectations(
                    Duration::from_millis(100),  // Max execution time
                    10.0  // Min operations per second
                )
        );
    
    let results = runner.run_all_benchmarks().await?;
    assert!(results[0].success);
    
    Ok(())
}
```

### Error Handling Test

```rust
#[tokio::test]
async fn test_invalid_command() -> TestResult<()> {
    test_container!(container);
    let runner = CliTestRunner::new(container);
    
    runner.run(&["invalid_command"])?
        .assert_failure()?
        .stderr_contains("error")?;
    
    Ok(())
}
```

## Running Tests

### Local Development

```bash
# Run all tests
cargo test

# Run only unit tests
cargo test --lib

# Run only integration tests
cargo test --test integration

# Run with verbose output
cargo test --verbose

# Run specific test
cargo test test_help_command

# Run tests with debug output
RUST_LOG=debug cargo test
```

### Test Categories

```bash
# Run CLI basic tests
cargo test cli_basic_tests

# Run command-specific tests
cargo test cli_command_tests

# Run performance tests
cargo test performance_tests

# Run error handling tests
cargo test error_handling_tests

# Run database operation tests
cargo test database_operation_tests
```

### Performance Testing

```bash
# Run performance benchmarks
cargo test test_cli_performance_benchmarks --release

# Run load tests
cargo test test_load_testing --release

# Run memory usage tests
cargo test test_memory_usage_tracking
```

## Performance Testing

### Benchmark Creation

```rust
let benchmark = PerformanceBenchmark::simple("command_name", "command")
    .with_expectations(
        Duration::from_millis(500),  // Maximum execution time
        2.0                          // Minimum throughput (ops/sec)
    )
    .with_setup("echo 'setup command'");
```

### Load Testing

```rust
let load_config = LoadTestConfig {
    concurrent_users: 10,
    test_duration: Duration::from_secs(30),
    ramp_up_time: Duration::from_secs(5),
    operations_per_second: Some(100.0),
    think_time: Duration::from_millis(10),
};

let result = runner.run_load_test(load_config).await?;
```

### Performance Metrics

The performance testing framework collects:

- **Execution Time**: Mean, median, min, max, standard deviation
- **Throughput**: Operations per second
- **Percentiles**: 95th and 99th percentile response times
- **Resource Usage**: Memory, CPU, disk I/O

## CI/CD Integration

### GitHub Actions Workflow

The `.github/workflows/cli-testing.yml` file defines comprehensive CI/CD testing:

1. **Multi-OS Testing**: Ubuntu, Windows, macOS
2. **Rust Version Testing**: Stable and beta channels
3. **Code Quality**: Formatting, linting, security audit
4. **Performance Testing**: Automated benchmarks
5. **Coverage Reporting**: Code coverage analysis
6. **E2E Testing**: Complete workflow validation

### Quality Gates

Tests must pass these quality gates:

- **Unit Tests**: >95% pass rate
- **Integration Tests**: 100% pass rate for critical paths
- **Performance**: No regression in key metrics
- **Security**: No known vulnerabilities
- **Coverage**: >80% code coverage
- **Formatting**: Consistent code style

## Best Practices

### 1. Test Organization

- **Arrange-Act-Assert**: Structure tests clearly
- **Single Responsibility**: One test per behavior
- **Descriptive Names**: Test names explain what and why
- **Independent Tests**: No dependencies between tests

### 2. Fixture Management

- **Realistic Data**: Use representative test data
- **Minimal Fixtures**: Only create necessary data
- **Reusable Fixtures**: Share common test data
- **Cleanup**: Automatic cleanup via TestContainer

### 3. Assertion Patterns

```rust
// Good: Specific assertions
runner.run(&["--help"])?
    .assert_success()?
    .stdout_contains("Usage:")?
    .stdout_contains("Options:")?;

// Good: Chain assertions
result.output()
    .stdout_is_json()?
    .has_field("version")?
    .field_equals("status", serde_json::Value::String("ok".to_string()))?;
```

### 4. Error Testing

- **Test Error Conditions**: Validate error paths
- **Graceful Failures**: Ensure graceful error handling
- **Error Messages**: Validate helpful error messages
- **Recovery**: Test recovery from errors

### 5. Performance Considerations

- **Baseline Metrics**: Establish performance baselines
- **Regression Detection**: Catch performance regressions
- **Resource Monitoring**: Track memory and CPU usage
- **Load Testing**: Test under realistic loads

### 6. Debugging Tests

```rust
// Enable debug logging
RUST_LOG=debug cargo test test_name

// Keep test artifacts for debugging
let config = TestConfig::new().no_cleanup();
test_container!(container, config);

// Add debug prints in tests
println!("Test environment: {:?}", container.environment());
```

## Test Utilities Reference

### Macros

- `test_container!(name)` - Create test container
- `assert_cli_success!(cmd)` - Assert CLI success
- `assert_cli_error!(cmd)` - Assert CLI error
- `assert_json_field!(json, field, value)` - Assert JSON field
- `assert_table_cell!(table, row, col, value)` - Assert table cell

### Assertion Methods

- `assert_success()` - Command succeeded
- `assert_failure()` - Command failed
- `stdout_contains(text)` - Output contains text
- `stderr_contains(text)` - Error contains text
- `stdout_is_json()` - Output is valid JSON
- `stdout_is_table()` - Output is table format
- `execution_time_less_than(duration)` - Performance check

### Configuration Options

```rust
let config = TestConfig::new()
    .with_timeout(Duration::from_secs(60))
    .with_parallel()
    .with_verbose()
    .with_temp_dir("/custom/temp")
    .no_cleanup();
```

## Troubleshooting

### Common Issues

1. **Test Timeouts**
   - Increase timeout in TestConfig
   - Check for deadlocks or infinite loops
   - Verify test dependencies

2. **Fixture Creation Failures**
   - Check file permissions
   - Verify directory structure
   - Validate fixture content format

3. **Assertion Failures**
   - Use verbose output to debug
   - Check exact output format
   - Verify test assumptions

4. **CI/CD Failures**
   - Check platform-specific issues
   - Verify dependencies are available
   - Review CI logs for details

### Debug Tips

```rust
// Print test environment details
let env = container.environment();
println!("Temp dir: {:?}", env.temp_dir);
println!("DB path: {:?}", env.db_path);

// Enable detailed logging
env_logger::init();

// Use no-cleanup for post-test inspection
let config = TestConfig::new().no_cleanup();
```

## Contributing

When adding new tests:

1. **Follow Patterns**: Use existing test patterns
2. **Add Documentation**: Document complex test scenarios
3. **Update CI**: Add new test categories to CI if needed
4. **Performance Impact**: Consider test execution time
5. **Cross-Platform**: Ensure tests work on all platforms

For questions or improvements to the testing infrastructure, please open an issue or submit a pull request.