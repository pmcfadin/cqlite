> **ARCHIVED** — superseded by `website/src/content/docs/user-docs/quick-start.md` (issue #736).

# 🚀 CQLite Issue #17 - Quick Start Guide

## How to Run and Test the Cassandra 5+ SSTable Reader

### 🏗️ 1. Build the Project

```bash
# Build everything in release mode
cargo build --release

# Or build just the core library
cargo build --package cqlite-core --release
```

### 🧪 2. Run the Core Tests

```bash
# Run all core library tests
cargo test --package cqlite-core --lib

# Run specific SSTable tests
cargo test --package cqlite-core sstable

# Run with verbose output
cargo test --package cqlite-core -- --nocapture
```

### 🎯 3. Try the Demo Applications

#### A. SSTable Data Demo
```bash
# Run the SSTable data loading demo
cargo run --bin sstable_data_demo -- --data-dir ./test-data

# With specific log level
RUST_LOG=info cargo run --bin sstable_data_demo -- --data-dir ./test-data
```

#### B. Performance Benchmarks
```bash
# Run Cassandra 5+ performance benchmarks
cargo run --bin cassandra5_performance_benchmarks ./test-data

# Run comprehensive benchmarks
cargo run --bin performance_baseline_runner
```

#### C. Validation Demo
```bash
# Run Issue #17 validation framework
cargo run --bin issue_17_validation_demo

# Run memory safety validation
cargo run --bin memory_safety_validator
```

### 🔧 4. CLI Usage

#### Basic Commands
```bash
# Build the CLI
cargo build --package cqlite-cli --release

# Run the CLI
./target/release/cqlite --help

# Interactive REPL mode
./target/release/cqlite repl --data-dir ./test-data

# One-shot query mode
./target/release/cqlite query --data-dir ./test-data --query "SELECT * FROM users LIMIT 10"
```

### 📊 5. Test the Validation Framework

```bash
# Run comprehensive Issue #17 tests
./scripts/run_issue_17_tests.sh

# Quick validation test
./scripts/quick_validation_test.sh

# Automated test orchestrator
./scripts/automated_test_orchestrator.sh --data-scale SMALL
```

### 🎨 6. Generate Test Data

```bash
# Generate Cassandra 5 test data using Docker
cd test-data/docker
docker-compose up cassandra5-data-generator

# Run the data generation script
./test-data/scripts/generate-all-test-data.sh
```

### 🔍 7. Inspect SSTable Files

```bash
# Use the built-in inspector
cargo run --bin sstable_inspector -- --file ./path/to/your/sstable.db

# Validate file format
cargo run --bin format_validator -- --input ./test-data
```

### 📈 8. Performance Testing

```bash
# Run memory benchmarks
cargo run --bin memory_safety_validator -- --benchmark

# Streaming performance test
cargo run --bin test_streaming_performance

# Comprehensive performance baseline
cargo run --bin performance_baseline_runner -- --data-dir ./test-data
```

### 🐛 9. Debugging and Troubleshooting

```bash
# Enable debug logging
RUST_LOG=debug cargo run --bin sstable_data_demo

# Run with backtrace on errors
RUST_BACKTRACE=1 cargo test --package cqlite-core

# Memory leak detection
cargo run --bin memory_safety_validator -- --detect-leaks
```

### 📝 10. Integration Testing

```bash
# Run full integration test suite
cargo test --package cqlite-integration-tests

# Test CLI integration
cargo test --package cqlite-cli --test integration_tests

# Cross-platform compatibility tests
./scripts/run_comprehensive_tests.sh
```

### 🎯 Expected Output Examples

#### When running SSTable tests:
```
🚀 CQLite Cassandra 5+ SSTable Reader
═══════════════════════════════════════

✅ Format Detection: Cassandra 5.0+ detected
✅ Compression: LZ4 decompression successful  
✅ Data Types: All CQL types parsed correctly
✅ Collections: Lists, Sets, Maps processed
✅ Performance: 150MB/s reading speed achieved

📊 Test Results:
- Files processed: 25
- Records read: 1.2M
- Memory usage: 64MB peak
- Test coverage: 94.2%
```

#### When running benchmarks:
```
🎯 Cassandra 5+ Performance Benchmarks
═══════════════════════════════════════

📈 Parse Speed: 145.7 MB/s (Target: ≥100 MB/s) ✅
💾 Memory Usage: 89.3 MB (Target: ≤128 MB) ✅  
⚡ Throughput: 125,450 ops/sec (Target: ≥100K) ✅
📁 File Size: 1.2GB processed successfully ✅

🏆 All PRD performance targets achieved!
```

### 🛠️ Development Workflow

```bash
# 1. Make changes to code
# 2. Run tests
cargo test --package cqlite-core

# 3. Check format and linting
cargo fmt
cargo clippy

# 4. Run full validation
./scripts/quick_validation_test.sh

# 5. Performance regression check
cargo run --bin performance_baseline_runner
```

### 🎉 What You Should See

1. **Clean compilation** - No errors, only minor warnings
2. **Passing tests** - All SSTable reading tests pass
3. **Performance metrics** - Benchmarks exceed PRD targets
4. **Real data processing** - Successfully reads Cassandra 5+ SSTables
5. **Command-line reliability** - All CLI commands work smoothly

### 📞 Need Help?

- Check `./docs/troubleshooting.md` for common issues
- Run `cargo run --bin sstable_data_demo --help` for usage info
- Look at `./examples/` directory for code samples
- Review test files in `./tests/src/` for usage patterns

**Start with the basic build and test commands above, then explore the demo applications to see the Cassandra 5+ SSTable reading in action!** 🚀