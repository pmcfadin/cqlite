# CQLite Testing Architecture

## Overview

This document outlines the recommended testing architecture for the CQLite CLI application, building upon the existing comprehensive test infrastructure.

## Directory Structure

```
cqlite/
├── cqlite-cli/
│   ├── src/
│   │   └── *.rs                 # Unit tests in #[cfg(test)]
│   └── tests/                   # CLI-specific integration tests
│       ├── integration_tests.rs
│       ├── error_handling_tests.rs
│       └── end_to_end_tests.rs
├── tests/                       # Workspace-level tests
│   ├── cli/                     # NEW: CLI testing module
│   │   ├── commands/            # Individual command tests
│   │   │   ├── parse_tests.rs
│   │   │   ├── query_tests.rs
│   │   │   ├── repl_tests.rs
│   │   │   └── export_tests.rs
│   │   ├── integration/         # CLI integration tests
│   │   │   ├── workflow_tests.rs
│   │   │   ├── error_scenarios.rs
│   │   │   └── performance_tests.rs
│   │   └── snapshots/           # Output snapshot tests
│   │       ├── query_outputs/
│   │       └── error_messages/
│   ├── fixtures/                # Test data (existing)
│   │   ├── sstables/
│   │   ├── golden/              # Expected outputs
│   │   └── generated/           # Generated test data
│   ├── property/                # NEW: Property-based tests
│   │   ├── parser_properties.rs
│   │   └── cli_properties.rs
│   └── helpers/                 # Test utilities
│       ├── mock_servers.rs
│       ├── test_data.rs
│       └── cli_assertions.rs
├── benches/                     # Performance benchmarks
│   ├── cli_benchmarks.rs        # NEW: CLI-specific benchmarks
│   ├── parser_benchmarks.rs
│   └── end_to_end_benchmarks.rs
└── examples/                    # Usage examples that serve as tests
    ├── cli_usage_examples.rs
    └── integration_examples.rs
```

## Test Categories

### 1. Unit Tests
**Location**: Within source files (`#[cfg(test)]` modules)  
**Purpose**: Test individual functions and structs  
**Tools**: Built-in `#[test]`, mockall for dependencies  

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;

    #[test]
    fn test_parse_cql_query() {
        let query = "SELECT * FROM users WHERE id = 1";
        let result = parse_cql_query(query);
        assert!(result.is_ok());
    }
}
```

### 2. CLI Integration Tests
**Location**: `tests/cli/`  
**Purpose**: Test CLI commands end-to-end  
**Tools**: assert_cmd, predicates, tempfile  

```rust
// tests/cli/commands/parse_tests.rs
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;

#[test]
fn test_parse_command_success() -> Result<(), Box<dyn std::error::Error>> {
    let mut cmd = Command::cargo_bin("cqlite")?;
    cmd.arg("parse")
       .arg("tests/fixtures/sstables/simple.sstable");
    
    cmd.assert()
       .success()
       .stdout(predicate::str::contains("Parsed successfully"));
    
    Ok(())
}
```

### 3. Property-Based Tests
**Location**: `tests/property/`  
**Purpose**: Test with generated inputs to find edge cases  
**Tools**: proptest  

```rust
// tests/property/cli_properties.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn cli_never_panics_on_invalid_input(
        invalid_data in prop::collection::vec(any::<u8>(), 0..1024)
    ) {
        // Test that CLI handles invalid input gracefully
        let temp_file = create_temp_file_with_data(&invalid_data);
        let result = run_cqlite_parse(&temp_file);
        
        // Should return error, not panic
        assert!(result.is_ok() || result.unwrap_err().code().is_some());
    }
}
```

### 4. Snapshot Tests
**Location**: `tests/cli/snapshots/`  
**Purpose**: Validate CLI output format consistency  
**Tools**: insta  

```rust
// tests/cli/snapshots/query_outputs.rs
use insta::assert_snapshot;

#[test]
fn test_json_output_format() {
    let output = run_cqlite_query("SELECT * FROM users", "--format=json");
    assert_snapshot!(output.stdout);
}
```

### 5. Performance Tests
**Location**: `benches/cli_benchmarks.rs`  
**Purpose**: Measure and track CLI performance  
**Tools**: criterion  

```rust
// benches/cli_benchmarks.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_parse_large_file(c: &mut Criterion) {
    c.bench_function("parse 100MB sstable", |b| {
        b.iter(|| {
            run_cqlite_parse(black_box("tests/fixtures/large.sstable"))
        })
    });
}
```

## Testing Patterns

### CLI Command Testing Pattern

```rust
use assert_cmd::prelude::*;
use predicates::prelude::*;
use std::process::Command;
use tempfile::TempDir;

pub struct CLITestHarness {
    temp_dir: TempDir,
}

impl CLITestHarness {
    pub fn new() -> Result<Self, std::io::Error> {
        Ok(Self {
            temp_dir: TempDir::new()?,
        })
    }
    
    pub fn run_command(&self, args: &[&str]) -> assert_cmd::assert::Assert {
        let mut cmd = Command::cargo_bin("cqlite").unwrap();
        cmd.args(args);
        cmd.assert()
    }
    
    pub fn create_test_file(&self, name: &str, content: &[u8]) -> std::path::PathBuf {
        let file_path = self.temp_dir.path().join(name);
        std::fs::write(&file_path, content).unwrap();
        file_path
    }
}

#[test]
fn test_interactive_mode() -> Result<(), Box<dyn std::error::Error>> {
    let harness = CLITestHarness::new()?;
    
    harness.run_command(&["--interactive"])
        .write_stdin("SELECT * FROM users;\n.quit\n")
        .success()
        .stdout(predicate::str::contains("cqlite>"));
    
    Ok(())
}
```

### Error Testing Pattern

```rust
#[test]
fn test_file_not_found_error() -> Result<(), Box<dyn std::error::Error>> {
    Command::cargo_bin("cqlite")?
        .arg("parse")
        .arg("nonexistent.sstable")
        .assert()
        .failure()
        .code(1)
        .stderr(predicate::str::contains("File not found"));
    
    Ok(())
}
```

### Performance Testing Pattern

```rust
use criterion::{BatchSize, Criterion};
use std::time::Duration;

fn benchmark_with_different_file_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("parse_by_file_size");
    
    for size in [1024, 10240, 102400].iter() {
        group.bench_with_input(
            BenchmarkId::new("parse", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || generate_test_sstable(size),
                    |test_file| run_cqlite_parse(&test_file),
                    BatchSize::SmallInput,
                )
            },
        );
    }
    
    group.finish();
}
```

## Test Data Management

### Fixture Organization

```
tests/fixtures/
├── sstables/
│   ├── minimal/              # Smallest valid files
│   │   ├── empty.sstable
│   │   └── single_row.sstable
│   ├── typical/              # Common use cases
│   │   ├── users_table.sstable
│   │   └── events_table.sstable
│   ├── complex/              # Advanced features
│   │   ├── collections.sstable
│   │   ├── user_defined_types.sstable
│   │   └── large_partitions.sstable
│   ├── edge_cases/           # Boundary conditions
│   │   ├── max_column_count.sstable
│   │   ├── unicode_data.sstable
│   │   └── compression_variants.sstable
│   └── corrupted/            # Error conditions
│       ├── truncated_header.sstable
│       ├── invalid_checksum.sstable
│       └── malformed_data.sstable
├── golden/                   # Expected outputs
│   ├── json_outputs/
│   ├── csv_outputs/
│   └── error_messages/
└── schemas/                  # Schema definitions
    ├── simple_table.cql
    ├── complex_table.cql
    └── system_tables.cql
```

### Test Data Generation

```rust
// tests/helpers/test_data.rs
pub struct SSTableGenerator {
    rng: StdRng,
}

impl SSTableGenerator {
    pub fn new() -> Self {
        Self {
            rng: StdRng::seed_from_u64(42), // Deterministic for reproducibility
        }
    }
    
    pub fn generate_simple_table(&mut self, rows: usize) -> Vec<u8> {
        // Generate reproducible test data
    }
    
    pub fn generate_with_schema(&mut self, schema: &TableSchema, rows: usize) -> Vec<u8> {
        // Generate data matching specific schema
    }
}
```

## CI/CD Integration

### GitHub Actions Workflow

```yaml
name: CLI Testing Pipeline

on: [push, pull_request]

jobs:
  cli-tests:
    name: CLI Tests
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, windows-latest, macos-latest]
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Install Rust
      uses: dtolnay/rust-toolchain@stable
      with:
        components: llvm-tools-preview
    
    - name: Install testing tools
      run: |
        cargo install cargo-nextest
        cargo install cargo-llvm-cov
    
    - name: Run unit tests
      run: cargo nextest run --lib --bins
    
    - name: Run CLI integration tests
      run: cargo nextest run --test 'cli_*'
    
    - name: Run property-based tests
      run: cargo nextest run --test 'property_*'
    
    - name: Generate coverage report
      run: cargo llvm-cov nextest --lcov --output-path lcov.info
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: lcov.info

  benchmarks:
    name: Performance Benchmarks
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    
    steps:
    - uses: actions/checkout@v4
    - name: Run CLI benchmarks
      run: cargo bench --bench cli_benchmarks
    - name: Store results
      uses: benchmark-action/github-action-benchmark@v1
      with:
        tool: 'cargo'
        output-file-path: target/criterion/cli_benchmarks/base/benchmark.json
```

## Quality Gates

### Coverage Requirements
- **Minimum Coverage**: 70% line coverage
- **Target Coverage**: 85% line coverage
- **Critical Paths**: 95% coverage for CLI command parsing

### Performance Thresholds
- **Regression Alert**: >10% slowdown
- **Response Time**: <500ms for typical operations
- **Memory Usage**: <100MB for standard operations

### Test Success Requirements
- **Unit Tests**: 100% pass rate
- **Integration Tests**: 100% pass rate
- **Property Tests**: 100% pass rate (with sufficient iterations)

## Implementation Steps

### Phase 1: Foundation
1. Add recommended dependencies to `Cargo.toml`
2. Create `tests/cli/` directory structure
3. migrate existing CLI tests to new structure
4. Implement basic command tests

### Phase 2: Enhancement
1. Add property-based tests for robust edge case coverage
2. Implement snapshot testing for output validation
3. Create comprehensive fixture library
4. Add performance benchmarks

### Phase 3: Integration
1. Update CI pipeline with new testing tools
2. Implement quality gates and coverage requirements
3. Add automated performance regression detection
4. Create test documentation and guidelines

This architecture builds upon CQLite's existing sophisticated testing infrastructure while adding modern CLI-specific testing patterns and comprehensive quality assurance.