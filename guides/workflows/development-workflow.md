# CQLite Development Workflow

## 🎯 Standard Development Process

This document outlines the recommended development workflow for contributing to CQLite, ensuring code quality and maintaining project standards.

## 🏗️ Development Environment Setup

### **Prerequisites**
```bash
# Rust toolchain (stable + nightly for benchmarks)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup toolchain install nightly
rustup component add rustfmt clippy

# Development tools
cargo install cargo-watch cargo-criterion cargo-flamegraph

# Cassandra 5 for testing
docker pull cassandra:5.0
```

### **Initial Setup**
```bash
# Clone repository
git clone https://github.com/apache/cqlite
cd cqlite

# Setup git hooks
./scripts/setup-hooks.sh

# Verify environment
cargo check --all-features
cargo test --workspace
```

## 📋 Feature Development Workflow

### **1. Issue Creation**
Before starting work, ensure an issue exists:
```markdown
Title: [Feature] Implement partition index parsing
Labels: enhancement, parser
Milestone: Phase 1 - Core Parsing

Description:
- Clear description of the feature
- Acceptance criteria
- Technical approach
- Test requirements
```

### **2. Branch Creation**
```bash
# Feature branches
git checkout -b feature/partition-index-parsing

# Bug fixes
git checkout -b fix/compression-boundary-error

# Documentation
git checkout -b docs/sstable-format-guide
```

### **3. Development Cycle**

#### **Test-Driven Development**
```rust
// 1. Write test first
#[test]
fn test_partition_index_parsing() {
    let test_data = include_bytes!("../test-data/partition-index.db");
    let index = PartitionIndex::parse(test_data).unwrap();
    
    assert_eq!(index.entry_count(), 1000);
    assert!(index.lookup(b"test_key").is_some());
}

// 2. Implement feature
pub fn parse(data: &[u8]) -> Result<PartitionIndex> {
    // Implementation
}

// 3. Refactor for clarity and performance
```

#### **Continuous Testing**
```bash
# Watch mode for rapid feedback
cargo watch -x check -x test

# Run specific test suite
cargo test --package cqlite-core --test parser_tests

# Benchmark during development
cargo criterion --bench parser_bench
```

### **4. Code Quality Checks**

#### **Before Committing**
```bash
# Format code
cargo fmt --all

# Lint checks
cargo clippy --all-features -- -D warnings

# Security audit
cargo audit

# Documentation check
cargo doc --no-deps --document-private-items
```

#### **Performance Validation**
```bash
# Run benchmarks
cargo bench --bench sstable_parsing

# Generate flamegraph
cargo flamegraph --bench sstable_parsing -o flamegraph.svg

# Check binary size
cargo bloat --release
```

## 🔄 Pull Request Process

### **1. PR Preparation**
```bash
# Ensure branch is up to date
git fetch origin
git rebase origin/main

# Run full test suite
./scripts/pre-pr-check.sh
```

### **2. PR Template**
```markdown
## Description
Brief description of changes

## Related Issue
Fixes #123

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Benchmarks show no regression
- [ ] Tested with real Cassandra 5 data

## Checklist
- [ ] Code follows project style
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
```

### **3. Review Process**
- Minimum 2 approvals required
- CI must pass (tests, lints, benchmarks)
- No merge conflicts
- Documentation complete

## 🧪 Testing Strategy

### **Unit Testing**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Property-based testing
    proptest! {
        #[test]
        fn roundtrip_serialization(value: CQLValue) {
            let serialized = value.serialize();
            let deserialized = CQLValue::deserialize(&serialized)?;
            prop_assert_eq!(value, deserialized);
        }
    }
}
```

### **Integration Testing**
```rust
// tests/integration/sstable_compatibility.rs
#[test]
fn test_cassandra5_compatibility() {
    let cassandra_file = TestData::cassandra5_sstable();
    let parsed = SSTable::parse(&cassandra_file).unwrap();
    
    // Verify against known values
    assert_eq!(parsed.partition_count(), 10000);
    assert_eq!(parsed.format_version(), "oa");
}
```

### **Benchmark Testing**
```rust
use criterion::{black_box, criterion_group, Criterion};

fn benchmark_parsing(c: &mut Criterion) {
    let data = include_bytes!("../benches/data/large.db");
    
    c.bench_function("parse_1gb_sstable", |b| {
        b.iter(|| {
            SSTable::parse(black_box(data))
        });
    });
}
```

## 📊 Performance Monitoring

### **Benchmark Tracking**
```toml
# benchmarks.toml
[[benchmark]]
name = "parse_1gb_sstable"
baseline = "850ms"
threshold = "5%"  # Fail if >5% regression
```

### **Memory Profiling**
```bash
# Using valgrind
valgrind --tool=massif --massif-out-file=massif.out \
    ./target/release/cqlite-bench

# Analyze results
ms_print massif.out > memory-usage.txt
```

## 🚀 Release Process

### **Version Bumping**
```bash
# Update version in Cargo.toml files
./scripts/bump-version.sh 0.2.0

# Update CHANGELOG.md
./scripts/generate-changelog.sh
```

### **Release Checklist**
- [ ] All tests passing
- [ ] Benchmarks show no regression
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped
- [ ] Git tag created
- [ ] Crates published

### **Publishing**
```bash
# Dry run first
cargo publish --dry-run -p cqlite-core

# Publish in dependency order
cargo publish -p cqlite-core
cargo publish -p cqlite-ffi
cargo publish -p cqlite-wasm
```

## 🐛 Debugging Techniques

### **SSTable Analysis**
```bash
# Hex dump for format analysis
xxd -l 1000 test.db | less

# Use debug builds with trace logging
RUST_LOG=cqlite=trace cargo run --bin cqlite -- parse test.db
```

### **Parser Debugging**
```rust
// Enable parser trace
#[cfg(debug_assertions)]
nom::trace::trace(
    "partition_parser",
    nom::combinator::all_consuming(partition_parser)
)(input)
```

## 📚 Documentation Standards

### **Code Documentation**
```rust
/// Parses a partition index from raw bytes.
/// 
/// # Arguments
/// * `data` - Raw bytes containing the partition index
/// 
/// # Returns
/// * `Ok(PartitionIndex)` - Successfully parsed index
/// * `Err(ParseError)` - Parse failure with context
/// 
/// # Example
/// ```
/// let data = std::fs::read("index.db")?;
/// let index = PartitionIndex::parse(&data)?;
/// ```
pub fn parse(data: &[u8]) -> Result<PartitionIndex, ParseError> {
    // Implementation
}
```

### **Module Documentation**
```rust
//! # Partition Index Module
//! 
//! This module provides parsing and querying capabilities for Cassandra
//! partition index files. The partition index maps partition keys to their
//! locations within the data file.
//! 
//! ## Format
//! The index file consists of:
//! - Header: 16 bytes
//! - Index entries: Variable length
//! - Footer: 8 bytes
```

---

*Following this workflow ensures consistent, high-quality contributions to the CQLite project while maintaining performance and compatibility standards.*