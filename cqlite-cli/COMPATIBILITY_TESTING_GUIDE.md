# 🐘 CQLite Cassandra Compatibility Testing Guide

This comprehensive guide covers the automated compatibility testing framework that ensures CQLite remains compatible with all Cassandra versions as they evolve.

## 🎯 Overview

The compatibility testing framework automatically:

- ✅ **Tests multiple Cassandra versions** (4.0, 4.1, 5.0, 5.1, 6.0+)
- 🔍 **Detects SSTable format changes** between versions
- 📊 **Validates data integrity** across versions
- ⚡ **Measures performance** and identifies regressions
- 🚨 **Alerts on compatibility issues** via CI/CD
- 📈 **Tracks trends** over time for proactive maintenance

## 🚀 Quick Start

### 1. Setup (One-time)

```bash
# Clone and setup the testing environment
cd cqlite-cli
./tests/compatibility/scripts/setup.sh
```

### 2. Run Tests

```bash
# Test specific Cassandra version
./tests/compatibility/scripts/compatibility_checker.sh --version 5.1 --test-suite comprehensive

# Run full compatibility matrix
./tests/compatibility/scripts/compatibility_checker.sh --matrix

# Monitor continuously (for production)
./tests/compatibility/scripts/compatibility_checker.sh --monitor 24h
```

### 3. View Results

```bash
# Open the compatibility dashboard
open tests/compatibility/results/compatibility-dashboard.md

# View detailed JSON results
cat tests/compatibility/results/compatibility-matrix.json
```

## 🏗️ Architecture

### Core Components

#### 1. **Version Manager** (`src/version_manager.rs`)
- Docker-based Cassandra version management
- Automatic setup/teardown of test clusters
- Health checking and readiness validation
- Port management for parallel testing

```rust
let mut manager = CassandraVersionManager::new();
let version_info = manager.start_version("5.1").await?;
let results = manager.run_compatibility_matrix().await?;
```

#### 2. **Format Detective** (`src/format_detective.rs`)
- Binary analysis of SSTable files
- Automatic detection of format changes
- Impact assessment (breaking vs. compatible)
- Format evolution tracking

```rust
let detective = FormatDetective::new();
let format = detective.analyze_sstable_format(&sstable_path).await?;
let diff = detective.compare_formats("4.0", "5.1")?;
```

#### 3. **Data Generator** (`src/data_generator.rs`)
- Deterministic test data across versions
- All CQL data types (basic, collections, UDTs)
- Complex nested structures
- Performance datasets

```rust
let mut generator = TestDataGenerator::new(output_dir);
let datasets = generator.generate_all_versions().await?;
```

#### 4. **Test Suite** (`src/suite.rs`)
- Comprehensive compatibility validation
- SSTable parsing verification
- Query compatibility testing
- Performance regression analysis

```rust
let mut suite = CompatibilityTestSuite::new(data_dir);
let report = suite.run_full_compatibility_suite().await?;
```

## 🧪 Test Levels

### Basic Testing
- Core SSTable parsing
- Basic data types (text, int, uuid)
- Simple queries
- **Runtime**: ~5 minutes

### Comprehensive Testing (Default)
- All CQL data types
- Collections (list, set, map)
- User-defined types (UDTs)
- Complex queries and indexes
- **Runtime**: ~20 minutes

### Full Testing
- All comprehensive tests
- Performance benchmarks
- Edge cases and stress testing
- Large dataset validation
- **Runtime**: ~60 minutes

## 📊 Supported Cassandra Versions

| Version | Status | SSTable Format | Key Features |
|---------|--------|----------------|--------------|
| 4.0.x | ✅ Stable | `big` | Baseline compatibility |
| 4.1.x | ✅ Stable | `big` | Virtual tables |
| 5.0.x | ✅ Stable | `big` | SAI indexes, Vector search |
| 5.1.x | ✅ Stable | `big` | Enhanced vector support |
| 6.0.x | 🔄 Testing | `big`* | Future features |

*Format may change in future versions

## 🔍 Format Change Detection

The framework automatically detects:

### Binary Format Changes
- Magic bytes modifications
- Header structure changes
- Metadata format evolution
- Compression algorithm updates

### Compatibility Impact Assessment
- **Fully Compatible**: No parser changes needed
- **Backward Compatible**: New features, old parsing works
- **Requires Update**: Parser modifications needed
- **Breaking**: Incompatible changes

### Example Detection

```bash
# Detect changes in new SSTable files
./compatibility_checker.sh detect --sstable-dir ./new-version-data --baseline 4.0

# Output:
🔍 Format changes detected:
  - ✅ New compression type: ZSTD
  - 🟡 Enhanced metadata format
  - ⚠️ Modified statistics layout (requires parser update)
```

## 🚨 Automated Monitoring

### CI/CD Integration

The framework includes a GitHub Actions workflow (`.github/workflows/cassandra-compatibility.yml`) that:

1. **Runs on every push/PR** - Catch compatibility issues early
2. **Daily scheduled runs** - Detect new Cassandra releases
3. **Matrix testing** - Parallel testing of all versions
4. **Automatic reporting** - PR comments with results
5. **Documentation updates** - Auto-update compatibility status

### Continuous Monitoring

```bash
# Monitor compatibility every 24 hours
./compatibility_checker.sh --monitor 24h --webhook "https://hooks.slack.com/..."

# Custom monitoring intervals
./compatibility_checker.sh --monitor 1h    # Hourly
./compatibility_checker.sh --monitor 7d    # Weekly
```

### Notifications

Set up notifications for compatibility issues:

```bash
# Slack webhook
export WEBHOOK_URL="https://hooks.slack.com/services/..."

# Email notifications (via webhook service)
export WEBHOOK_URL="https://api.mailgun.com/v3/..."
```

## 📈 Performance Monitoring

### Metrics Tracked

- **Parsing Speed**: Rows per second
- **Memory Usage**: Peak and average consumption
- **File Size**: SSTable compression ratios
- **Query Performance**: Execution time comparisons

### Regression Detection

The framework automatically identifies:

- Parsing speed degradation > 20%
- Memory usage increases > 50%
- Query performance regressions > 30%

### Performance Reports

```json
{
  "version": "5.1",
  "performance_metrics": {
    "parsing_time_ms": 1250,
    "memory_usage_mb": 45.2,
    "throughput_rows_per_sec": 8000.0,
    "baseline_comparison": 0.95
  }
}
```

## 🔧 Configuration

### Environment Variables

```bash
# Test configuration
export DEFAULT_TEST_SUITE=comprehensive
export OUTPUT_DIR=./results
export CASSANDRA_VERSIONS=4.0,4.1,5.0,5.1

# Performance thresholds
export MIN_COMPATIBILITY_SCORE=95
export PERFORMANCE_THRESHOLD=0.8

# Monitoring
export MONITOR_INTERVAL=24h
export WEBHOOK_URL=https://hooks.slack.com/...
```

### Docker Configuration

```yaml
# tests/compatibility/docker/docker-compose.yml
services:
  cassandra-4-0:
    image: cassandra:4.0
    ports: ["9042:9042"]
  cassandra-5-1:
    image: cassandra:5.1
    ports: ["9045:9042"]
```

## 📊 Report Formats

### JSON (Machine-readable)
```bash
./compatibility_checker.sh --matrix --format json
```

### Markdown (Human-readable)
```bash
./compatibility_checker.sh --matrix --format markdown
```

### Dashboard (Web-friendly)
```bash
# Generates compatibility-dashboard.md
./compatibility_checker.sh --matrix
```

## 🚨 Troubleshooting

### Common Issues

#### Docker Permission Errors
```bash
# Add user to docker group
sudo usermod -aG docker $USER
# Re-login or restart terminal
```

#### Cassandra Startup Timeouts
```bash
# Increase Docker memory
# Docker Desktop > Settings > Resources > Memory: 4GB+

# Check container logs
docker logs cassandra-5.1
```

#### Port Conflicts
```bash
# Check if ports are in use
lsof -i :9042

# Use custom ports
./compatibility_checker.sh --version 5.1 --port 9999
```

### Debug Mode

```bash
# Enable verbose output
./compatibility_checker.sh --version 5.1 --verbose

# Keep containers running for debugging
./compatibility_checker.sh --version 5.1 --no-cleanup
```

## 🛠️ Development

### Adding New Cassandra Versions

1. **Update version lists**:
```rust
// src/version_manager.rs
supported_versions: vec![
    "4.0".to_string(),
    "4.1".to_string(),
    "5.0".to_string(),
    "5.1".to_string(),
    "6.0".to_string(),  // Add new version
],
```

2. **Update Docker Compose**:
```yaml
# docker/docker-compose.yml
cassandra-6-0:
  image: cassandra:6.0
  ports: ["9046:9042"]
```

3. **Update CI workflow**:
```yaml
# .github/workflows/cassandra-compatibility.yml
matrix:
  include: [4.0, 4.1, 5.0, 5.1, 6.0]  # Add new version
```

### Adding New Test Cases

```rust
// src/data_generator.rs
fn generate_new_feature_test_cases(&self) -> Vec<TestCase> {
    vec![
        TestCase {
            name: "new_feature_test".to_string(),
            description: "Test new Cassandra feature".to_string(),
            insert_statements: vec![
                "CREATE TABLE test.new_feature (id UUID PRIMARY KEY, data NEW_TYPE)".to_string(),
            ],
            complexity_level: ComplexityLevel::Complex,
            cassandra_version_min: "6.0".to_string(),
        },
    ]
}
```

## 🎯 Future Enhancements

### Planned Features

1. **🤖 AI-Powered Analysis**
   - Automatic issue classification
   - Suggested parser fixes
   - Compatibility prediction

2. **📊 Advanced Analytics**
   - Trend analysis
   - Compatibility scoring evolution
   - Performance forecasting

3. **🔗 Integration Expansions**
   - DataStax Enterprise versions
   - ScyllaDB compatibility
   - Custom Cassandra builds

4. **⚡ Performance Optimizations**
   - Parallel format analysis
   - Incremental testing
   - Smart test selection

## 🤝 Contributing

### Running Tests

```bash
# Unit tests
cd tests/compatibility
cargo test

# Integration tests
./scripts/compatibility_checker.sh --version 4.0 --test-suite basic

# Full test suite
./scripts/compatibility_checker.sh --matrix --test-suite comprehensive
```

### Code Style

```bash
# Format code
cargo fmt

# Lint
cargo clippy

# Documentation
cargo doc --open
```

## 📚 Resources

- **Documentation**: `tests/compatibility/README.md`
- **Configuration**: `tests/compatibility/.env`
- **Scripts**: `tests/compatibility/scripts/`
- **Results**: `tests/compatibility/results/`
- **Docker Setup**: `tests/compatibility/docker/`

## 📞 Support

- **Issues**: Create GitHub issues for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Documentation**: Check inline code documentation
- **CI Status**: Monitor GitHub Actions for compatibility status

---

This compatibility testing framework ensures CQLite stays compatible with Cassandra's evolution, providing confidence for users and maintainers alike! 🚀