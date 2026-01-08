# Issue #29: 🧪 Implement comprehensive CLI testing framework

## 🎯 **Priority: MEDIUM** - Quality Assurance Foundation

**Status**: Basic testing infrastructure exists but needs comprehensive CLI coverage  
**Impact**: Ensures reliable CLI functionality and prevents regressions  
**Estimated Effort**: 4-5 days  
**Assigned**: TBD  

---

## 📋 **Problem Statement**

CQLite has extensive core library testing but lacks comprehensive CLI-specific testing. We need a robust testing framework that validates all CLI commands, options, error conditions, and user workflows to ensure reliable operation across platforms.

Current gaps:
- No systematic CLI command testing
- Missing integration tests for user workflows  
- No cross-platform CLI behavior validation
- Limited error condition and edge case testing
- No performance testing for CLI operations

## ✅ **Acceptance Criteria**

### **Unit Testing Framework**
- [ ] Individual CLI command testing with mocked dependencies
- [ ] Argument parsing and validation testing
- [ ] Configuration loading and merging testing
- [ ] Output formatting testing (text, JSON, CSV, YAML)
- [ ] Error message validation and consistency

### **Integration Testing Framework**
- [ ] End-to-end CLI workflow testing
- [ ] Real file I/O operations with temporary data
- [ ] Multi-command sequences and state persistence
- [ ] Performance testing with realistic datasets
- [ ] Cross-platform behavior validation

### **End-to-End Testing Framework**
- [ ] Complete user journey testing (discovery → query → export)  
- [ ] Real SSTable file processing
- [ ] REPL session testing with command sequences
- [ ] Error recovery and graceful degradation testing
- [ ] Resource usage and memory leak detection

### **Test Infrastructure**
- [ ] Automated test data generation and cleanup
- [ ] Parallel test execution capabilities
- [ ] Comprehensive assertion helpers for CLI output
- [ ] Performance benchmarking and regression detection
- [ ] CI/CD integration with quality gates

## 🔧 **Technical Implementation**

### **Test Dependencies**
```toml
# Cargo.toml additions for comprehensive CLI testing
[dev-dependencies]
assert_cmd = "2.0.14"           # CLI command testing
predicates = "3.1.0"            # Flexible assertions
tempfile = "3.8"                # Temporary file/directory management
criterion = "0.5"               # Performance benchmarking
proptest = "1.4"                # Property-based testing
insta = "1.34"                  # Snapshot testing for output
mockall = "0.12"                # Mocking framework
serial_test = "3.0"             # Sequential test execution
tokio-test = "0.4"              # Async testing utilities
rstest = "0.18"                 # Parameterized testing
```

### **CLI Test Utilities**
```rust
// tests/cli/mod.rs - CLI testing utilities
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use std::path::{Path, PathBuf};

pub struct CliTestHarness {
    temp_dir: TempDir,
    binary_path: PathBuf,
}

impl CliTestHarness {
    pub fn new() -> Result<Self> {
        let temp_dir = TempDir::new()?;
        let binary_path = assert_cmd::cargo::cargo_bin("cqlite");
        
        Ok(Self {
            temp_dir,
            binary_path,
        })
    }
    
    pub fn command(&self) -> Command {
        Command::new(&self.binary_path)
    }
    
    pub fn temp_path(&self) -> &Path {
        self.temp_dir.path()
    }
    
    pub async fn create_test_sstable(&self, name: &str) -> Result<PathBuf> {
        // Create realistic test SSTable files
        let sstable_path = self.temp_path().join(format!("{}.db", name));
        create_test_sstable_file(&sstable_path).await?;
        Ok(sstable_path)
    }
}

// CLI output assertion helpers
pub trait CliOutputAssertions {
    fn assert_success_with_output(&mut self, expected: &str) -> &mut Self;
    fn assert_json_output(&mut self) -> &mut Self;
    fn assert_table_output(&mut self) -> &mut Self;
    fn assert_error_with_message(&mut self, message: &str) -> &mut Self;
    fn assert_execution_time_under(&mut self, max_duration_ms: u64) -> &mut Self;
}

impl CliOutputAssertions for assert_cmd::assert::Assert {
    fn assert_success_with_output(&mut self, expected: &str) -> &mut Self {
        self.success().stdout(predicate::str::contains(expected))
    }
    
    fn assert_json_output(&mut self) -> &mut Self {
        self.success().stdout(predicate::str::is_match(r#"\{.*\}"#).unwrap())
    }
    
    fn assert_table_output(&mut self) -> &mut Self {
        self.success().stdout(predicate::str::contains("┌").or(predicate::str::contains("+")))
    }
    
    fn assert_error_with_message(&mut self, message: &str) -> &mut Self {
        self.failure().stderr(predicate::str::contains(message))
    }
    
    fn assert_execution_time_under(&mut self, max_duration_ms: u64) -> &mut Self {
        // Custom predicate to check execution time from output
        self.success().stdout(predicate::function(move |output: &str| {
            extract_execution_time(output).map_or(true, |time| time < max_duration_ms)
        }))
    }
}
```

### **Unit Tests for CLI Commands**
```rust
// tests/cli/commands/info_test.rs
use super::*;

#[tokio::test]
async fn test_info_command_basic_functionality() {
    let harness = CliTestHarness::new()?;
    let test_sstable = harness.create_test_sstable("basic_table").await?;
    
    harness.command()
        .arg("info")
        .arg(&test_sstable)
        .assert()
        .assert_success_with_output("SSTable Information")
        .assert_success_with_output("File Details")
        .assert_success_with_output("Format Details");
}

#[tokio::test]
async fn test_info_command_json_output() {
    let harness = CliTestHarness::new()?;
    let test_sstable = harness.create_test_sstable("json_test").await?;
    
    harness.command()
        .arg("info")
        .arg("--format")
        .arg("json")
        .arg(&test_sstable)
        .assert()
        .assert_json_output();
}

#[tokio::test]
async fn test_info_command_nonexistent_file() {
    let harness = CliTestHarness::new()?;
    
    harness.command()
        .arg("info")
        .arg("/nonexistent/file.db")
        .assert()
        .assert_error_with_message("File not found");
}
```

### **Integration Tests for Workflows**
```rust
// tests/cli/integration/workflows_test.rs
use super::*;

#[tokio::test]
async fn test_complete_data_exploration_workflow() {
    let harness = CliTestHarness::new()?;
    let test_sstable = harness.create_test_sstable("user_data").await?;
    
    // Step 1: Get basic info about the file
    harness.command()
        .arg("info")
        .arg(&test_sstable)
        .assert()
        .assert_success_with_output("users");
    
    // Step 2: Query the data
    harness.command()
        .arg("query")
        .arg("SELECT * FROM users LIMIT 5")
        .arg("--sstable")
        .arg(&test_sstable)
        .assert()
        .assert_success_with_output("rows returned");
        
    // Step 3: Export to different formats
    let export_file = harness.temp_path().join("export.json");
    harness.command()
        .arg("export")
        .arg("--format")
        .arg("json")
        .arg("--output")
        .arg(&export_file)
        .arg(&test_sstable)
        .assert()
        .success();
        
    // Verify export file was created and contains data
    assert!(export_file.exists());
    let content = std::fs::read_to_string(&export_file)?;
    assert!(content.contains("users"));
}

#[tokio::test]
async fn test_error_recovery_workflow() {
    let harness = CliTestHarness::new()?;
    
    // Test graceful handling of invalid file
    harness.command()
        .arg("info")
        .arg("/invalid/path.db")
        .assert()
        .assert_error_with_message("File not found")
        .assert_error_with_message("suggestions:");
        
    // Test recovery with valid file
    let test_sstable = harness.create_test_sstable("recovery_test").await?;
    harness.command()
        .arg("info")
        .arg(&test_sstable)
        .assert()
        .success();
}
```

### **End-to-End REPL Testing**
```rust
// tests/cli/e2e/repl_test.rs
use std::process::{Command, Stdio};
use std::io::Write;

#[tokio::test]
async fn test_repl_interactive_session() {
    let harness = CliTestHarness::new()?;
    let test_sstable = harness.create_test_sstable("repl_test").await?;
    
    let mut child = Command::new(harness.binary_path())
        .arg("repl")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    
    let stdin = child.stdin.as_mut().unwrap();
    
    // Test REPL command sequence
    writeln!(stdin, ":help")?;
    writeln!(stdin, ":info {}", test_sstable.display())?;
    writeln!(stdin, "SELECT * FROM users LIMIT 3")?;
    writeln!(stdin, ":config")?;
    writeln!(stdin, ":quit")?;
    
    let output = child.wait_with_output()?;
    
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    
    assert!(stdout.contains("Available commands"));
    assert!(stdout.contains("SSTable Information"));
    assert!(stdout.contains("Configuration"));
}
```

### **Performance Testing**
```rust
// tests/cli/performance/benchmarks.rs
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_info_command(c: &mut Criterion) {
    let harness = CliTestHarness::new().unwrap();
    
    c.bench_function("info command small file", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let test_file = harness.create_test_sstable("small").await.unwrap();
                black_box(
                    harness.command()
                        .arg("info")
                        .arg(&test_file)
                        .output()
                        .await
                        .unwrap()
                );
            });
    });
    
    c.bench_function("info command large file", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| async {
                let test_file = harness.create_large_test_sstable("large").await.unwrap();
                black_box(
                    harness.command()
                        .arg("info")
                        .arg(&test_file)
                        .output()
                        .await
                        .unwrap()
                );
            });
    });
}

criterion_group!(benches, bench_info_command);
criterion_main!(benches);
```

### **Property-Based Testing**
```rust
// tests/cli/property/argument_parsing.rs
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_info_command_never_panics_on_paths(
        path in r"[a-zA-Z0-9/._-]{1,100}"
    ) {
        let harness = CliTestHarness::new().unwrap();
        
        // Should never panic, only succeed or fail gracefully
        let result = harness.command()
            .arg("info")
            .arg(&path)
            .output();
            
        // Test passes if we get any result (success or failure)
        prop_assert!(result.is_ok());
    }
    
    #[test]
    fn test_query_command_sql_injection_safety(
        query in r"SELECT .{0,200} FROM .{0,50}[';\"\\]{0,10}.*"
    ) {
        let harness = CliTestHarness::new().unwrap();
        
        // Should handle malicious SQL gracefully
        let result = harness.command()
            .arg("query")
            .arg(&query)
            .output();
            
        prop_assert!(result.is_ok());
        // Should not execute arbitrary commands or cause crashes
    }
}
```

## 📊 **Test Organization Structure**

```
tests/
├── cli/                          # CLI-specific tests
│   ├── mod.rs                   # Common utilities and helpers
│   ├── commands/                # Individual command testing
│   │   ├── info_test.rs
│   │   ├── query_test.rs
│   │   ├── repl_test.rs
│   │   ├── export_test.rs
│   │   └── import_test.rs
│   ├── integration/             # Multi-command workflows
│   │   ├── workflows_test.rs
│   │   ├── error_handling_test.rs
│   │   └── configuration_test.rs
│   ├── e2e/                     # End-to-end user scenarios
│   │   ├── repl_sessions_test.rs
│   │   ├── data_pipeline_test.rs
│   │   └── cross_platform_test.rs
│   ├── performance/             # Performance benchmarks
│   │   ├── benchmarks.rs
│   │   └── memory_usage_test.rs
│   └── property/                # Property-based testing
│       ├── argument_parsing.rs
│       └── output_formatting.rs
├── fixtures/                    # Test data and fixtures
│   ├── sstables/               # Sample SSTable files
│   ├── schemas/                # Test schema definitions
│   └── expected_outputs/       # Golden files for output testing
└── support/                    # Test support utilities
    ├── test_data_generator.rs
    ├── assertion_helpers.rs
    └── mock_factories.rs
```

## 🔄 **CI/CD Integration**

### **GitHub Actions Workflow**
```yaml
# .github/workflows/comprehensive-cli-testing.yml
name: CLI Testing Suite

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  cli-tests:
    name: CLI Tests (${{ matrix.os }})
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        
    steps:
    - uses: actions/checkout@v4
    
    - name: Install Rust
      uses: dtolnay/rust-toolchain@stable
      
    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: |
          ~/.cargo/registry
          ~/.cargo/git
          target/
        key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
        
    - name: Build CLI binary
      run: cargo build --bin cqlite --release
      
    - name: Run unit tests
      run: cargo test --test cli_commands
      
    - name: Run integration tests  
      run: cargo test --test cli_integration
      
    - name: Run E2E tests
      run: cargo test --test cli_e2e
      
    - name: Run performance benchmarks
      run: cargo bench --bench cli_performance
      
    - name: Generate coverage report
      run: |
        cargo install cargo-llvm-cov
        cargo llvm-cov --html --output-dir coverage
        
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: coverage/html/index.html
```

## 📖 **Testing Best Practices**

### **Test Naming Convention**
```rust
// Pattern: test_{component}_{scenario}_{expected_outcome}
#[test]
fn test_info_command_valid_file_displays_metadata() { }

#[test]  
fn test_query_command_invalid_sql_returns_error() { }

#[test]
fn test_repl_session_help_command_shows_usage() { }
```

### **Assertion Patterns**
```rust
// Use descriptive assertions with clear error messages
assert!(
    result.contains("SSTable Information"),
    "Info command should display SSTable Information header, got: {}",
    result
);

// Use custom assertion helpers for common patterns
harness.command()
    .arg("info")
    .arg(&test_file)
    .assert()
    .assert_success_with_table_output()
    .assert_execution_time_under(2000);
```

### **Test Data Management**
```rust
// Use builders for complex test data
let test_sstable = TestSSTableBuilder::new()
    .with_keyspace("test_ks")
    .with_table("users")
    .with_rows(1000)
    .with_columns(vec!["id", "name", "email"])
    .build()?;
```

## 🚀 **Implementation Plan**

### **Phase 1: Foundation (Days 1-2)**
1. Set up test dependencies and basic CLI test utilities
2. Create CliTestHarness and assertion helpers
3. Implement basic command testing for info and query commands
4. Set up temporary test data generation

### **Phase 2: Comprehensive Coverage (Days 2-3)**
1. Add unit tests for all CLI commands and options
2. Implement integration tests for common workflows
3. Add error handling and edge case testing
4. Create property-based tests for argument parsing

### **Phase 3: Advanced Testing (Days 3-4)**
1. Implement E2E REPL session testing
2. Add performance benchmarking and regression detection
3. Create cross-platform compatibility tests
4. Add memory usage and resource leak detection

### **Phase 4: CI/CD Integration (Days 4-5)**
1. Set up comprehensive CI/CD testing pipeline
2. Add coverage reporting and quality gates
3. Implement automated test data generation
4. Complete documentation and testing guidelines

## 📊 **Success Metrics**

### **Coverage Metrics**
- [ ] Unit test coverage > 95% for CLI command logic
- [ ] Integration test coverage > 90% for user workflows
- [ ] E2E test coverage > 85% for complete scenarios
- [ ] Performance test coverage for all critical paths

### **Quality Metrics**
- [ ] Zero unhandled errors or panics in normal operation
- [ ] Consistent behavior across all supported platforms
- [ ] All CLI options and flags properly tested
- [ ] Comprehensive error message validation

### **Performance Metrics**
- [ ] All CLI operations complete within defined time limits
- [ ] Memory usage remains bounded for all operations
- [ ] Performance regression detection catches degradations
- [ ] Resource cleanup verified (no leaks)

## ⚠️ **Risk Factors**

- **Medium**: Cross-platform CLI behavior differences
- **Medium**: Test execution time for comprehensive suite
- **Low**: Test data generation reliability
- **Low**: CI/CD resource usage for parallel testing

## 💡 **Future Enhancements**

- Automated CLI documentation generation from tests
- User acceptance testing with real user scenarios
- Load testing for concurrent CLI operations
- Integration with external validation tools
- Fuzzing for security and robustness testing

---

**Labels**: `medium-priority`, `testing`, `cli`, `quality-assurance`, `phase-2`  
**Milestone**: Quality Infrastructure  
**Dependencies**: Basic CLI functionality (#24, #25, #26, #27)  
**Enables**: Reliable releases and user confidence