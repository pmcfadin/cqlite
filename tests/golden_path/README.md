# Golden-Path Testing Framework for CQLite

A comprehensive testing framework for validating CQLite's core operations using real Cassandra 5 SSTable artifacts.

## Overview

The Golden-Path Testing Framework provides systematic validation of:
- **get()** operations - Single key lookups with real data
- **scan()** operations - Range queries and full table scans
- **lookup_partition_with_index()** - Partition-based lookups with index usage
- **Component Integration** - End-to-end coordination across Summary, Index, and Data files

## Key Features

✅ **Real SSTable Artifacts** - Uses authentic Cassandra 5.x files for realistic testing
✅ **Component Integration** - Validates Summary→Index→Data coordination
✅ **Performance Benchmarks** - Tracks regression and optimization metrics
✅ **Golden Expectations** - Compares results against known good outputs
✅ **Comprehensive Coverage** - Systematic validation of happy-path scenarios

## Quick Start

### 1. Basic Usage

```rust
use cqlite::tests::golden_path::{GoldenPathTestSuite, GoldenPathConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create test configuration
    let config = GoldenPathConfig::default();

    // Initialize test suite
    let mut test_suite = GoldenPathTestSuite::new(config).await?;

    // Run all golden-path scenarios
    let results = test_suite.run_all_scenarios().await?;

    // Generate report
    let report = test_suite.generate_report(&results);
    println!("{}", report);

    Ok(())
}
```

### 2. Run Specific Test Categories

```rust
// Run only get operation tests
let get_results = test_suite.run_get_scenarios().await?;

// Run only scan operation tests
let scan_results = test_suite.run_scan_scenarios().await?;

// Run only partition lookup tests
let lookup_results = test_suite.run_lookup_scenarios().await?;

// Run only integration tests
let integration_results = test_suite.run_integration_scenarios().await?;
```

## Test Data Organization

### Directory Structure

```
tests/golden_path/
├── artifacts/                    # Real Cassandra 5 SSTable files
│   ├── simple_table/             # Basic single-partition tables
│   │   ├── users-*-Data.db      # User table with known data
│   │   ├── users-*-Index.db
│   │   ├── users-*-Summary.db
│   │   ├── schema.cql           # Table definition (optional)
│   │   └── test_data.json       # Known test data (optional)
│   ├── multi_partition/          # Multiple partitions for range testing
│   ├── wide_partitions/          # Large partitions for index efficiency
│   └── complex_types/            # Collections, UDTs, etc.
├── expected_outputs/             # Golden expectations
│   ├── users_get_results.json
│   ├── events_scan_results.json
│   └── sensors_range_results.json
└── benchmarks/                   # Performance baselines
    ├── get_operations.json
    ├── scan_operations.json
    └── lookup_operations.json
```

### Setting Up Test Artifacts

1. **Create artifact directory structure:**
```bash
cargo test golden_path::setup_artifacts
```

2. **Place real Cassandra 5 SSTable files:**
   - Copy `*-Data.db`, `*-Index.db`, `*-Summary.db` files to appropriate directories
   - Add schema definitions in `schema.cql` files
   - Create test data manifests in `test_data.json` files

3. **Verify artifacts:**
```bash
cargo test golden_path::verify_artifacts
```

## Test Scenarios

### Get Operations

| Scenario | Description | Expected Behavior |
|----------|-------------|-------------------|
| `get_single_key_existing` | Lookup known partition key | Returns expected value |
| `get_multiple_keys_mixed` | Batch lookup (existing + non-existing) | Mixed results |
| `get_nonexistent_key_bloom_filter` | Lookup non-existent key | Fast bloom filter rejection |
| `get_with_bloom_filter_validation` | Validate bloom filter efficiency | High cache hit rate |

### Scan Operations

| Scenario | Description | Expected Behavior |
|----------|-------------|-------------------|
| `scan_full_table` | Complete table scan | All partitions returned |
| `scan_token_range` | Range query with bounds | Partitions in range |
| `scan_with_limit` | Limited result scan | Respects limit |
| `scan_empty_range` | Range with no data | Empty results |

### Lookup Operations

| Scenario | Description | Expected Behavior |
|----------|-------------|-------------------|
| `lookup_partition_basic` | Basic index-based lookup | Efficient partition location |
| `lookup_partition_promoted_index` | Wide partition lookup | Uses promoted index |
| `lookup_wide_partition_efficiency` | Multiple wide partitions | Maintains performance |

### Integration Tests

| Scenario | Description | Expected Behavior |
|----------|-------------|-------------------|
| `summary_index_coordination` | Summary→Index integration | Proper coordination |
| `index_data_coordination` | Index→Data integration | Accurate data access |
| `end_to_end_coordination` | Complete chain validation | Full workflow success |

## Performance Tracking

### Metrics Collected

- **Latency**: Average, P95, P99 response times
- **Throughput**: Operations per second
- **Memory**: Peak usage during operations
- **Cache**: Hit/miss rates for bloom filters and indexes
- **Coordination**: Component interaction timing

### Regression Detection

The framework automatically detects performance regressions by comparing current results against historical baselines:

```rust
// Configure regression detection
let config = GoldenPathConfig {
    performance_threshold: 10.0, // 10% regression threshold
    detailed_metrics: true,
    ..Default::default()
};
```

### Benchmark Reports

```rust
// Generate performance report
let report = test_suite.generate_performance_report();
println!("Performance Status: {:?}", report.overall_health);

for (scenario, summary) in report.scenario_summaries {
    if summary.regression_analysis.has_regression {
        println!("⚠️  Regression in {}: {}%",
            scenario,
            summary.regression_analysis.regression_percentage);
    }
}
```

## Validation Framework

### Data Validation

The framework validates results using multiple criteria:

```rust
use cqlite::tests::golden_path::scenarios::ValidationCriteria;

// Exact value matching
ValidationCriteria::ExactMatch

// Substring containment
ValidationCriteria::Contains("expected_substring".to_string())

// Numeric range validation
ValidationCriteria::Range { min: 0, max: 100 }

// Custom validation functions
ValidationCriteria::Custom("custom_validator".to_string())
```

### Performance Validation

Performance requirements are validated against configurable thresholds:

```rust
use cqlite::tests::golden_path::scenarios::PerformanceRequirements;

let requirements = PerformanceRequirements {
    max_latency_ms: 100,        // 100ms max latency
    min_throughput: 100.0,      // 100 ops/sec minimum
    max_memory_kb: 1024,        // 1MB max memory
    min_cache_hit_rate: 0.8,    // 80% cache hit rate
};
```

## Configuration Options

### Test Configuration

```rust
use cqlite::tests::golden_path::GoldenPathConfig;

let config = GoldenPathConfig {
    artifacts_dir: PathBuf::from("tests/golden_path/artifacts"),
    performance_threshold: 10.0,   // % regression threshold
    detailed_metrics: true,        // Enable detailed tracking
    timeout: Duration::from_secs(30), // Max test time
    validate_integration: true,    // Enable integration tests
};
```

### Artifact Management

```rust
use cqlite::tests::golden_path::artifacts::ArtifactOrganization;

// Discover available test datasets
let artifacts = ArtifactOrganization::new(artifacts_dir).await?;
let datasets = artifacts.list_datasets();

for dataset in datasets {
    println!("Dataset: {} ({})", dataset.name, dataset.description);
    for (table_name, table_info) in &dataset.tables {
        println!("  Table: {} ({} partitions)", table_name, table_info.partition_count);
    }
}
```

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: Golden-Path Tests

on: [push, pull_request]

jobs:
  golden-path-tests:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4

    - name: Setup Rust
      uses: dtolnay/rust-toolchain@stable

    - name: Download test artifacts
      run: |
        # Download real Cassandra 5 SSTable files
        curl -L "https://artifacts.example.com/sstables.tar.gz" | tar -xz -C tests/golden_path/artifacts/

    - name: Run golden-path tests
      run: cargo test golden_path --release

    - name: Check for regressions
      run: |
        cargo run --bin golden_path_reporter -- --check-regressions --threshold 10
```

### Performance Monitoring

```bash
# Daily performance monitoring
cargo run --bin golden_path_monitor -- --daily-report

# Regression alerts
cargo run --bin golden_path_monitor -- --alert-on-regression --threshold 15
```

## Advanced Usage

### Custom Test Scenarios

```rust
use cqlite::tests::golden_path::scenarios::{TestScenario, TestOperation, TestExpectations};

// Create custom test scenario
let custom_scenario = TestScenario {
    name: "custom_bulk_lookup".to_string(),
    description: "Custom bulk partition lookup test".to_string(),
    table_name: "custom_table".to_string(),
    operation: TestOperation::Get {
        keys: custom_keys,
    },
    expectations: TestExpectations {
        should_succeed: true,
        expected_count: Some(100),
        expected_results: vec![],
        expected_errors: vec![],
    },
    performance_requirements: PerformanceRequirements {
        max_latency_ms: 50,
        min_throughput: 200.0,
        ..Default::default()
    },
};

// Execute custom scenario
let result = test_suite.execute_scenario(&custom_scenario).await?;
```

### Custom Validation

```rust
use cqlite::tests::golden_path::validation::{validate_scenario_result, ValidationResults};

// Custom validation logic
async fn custom_validator(
    scenario: &TestScenario,
    result: &ScenarioExecutionResult,
) -> ValidationResults {
    // Implement custom validation logic
    ValidationResults {
        data_correct: true,
        performance_acceptable: true,
        integration_valid: true,
        messages: vec!["Custom validation passed".to_string()],
    }
}
```

## Troubleshooting

### Common Issues

1. **Missing test artifacts:**
   ```
   Error: Artifact set 'simple_table' not found
   ```
   **Solution:** Ensure SSTable files are placed in the correct directory structure.

2. **Performance regressions:**
   ```
   Warning: Latency regression detected: 150ms > 100ms (50% regression)
   ```
   **Solution:** Investigate code changes and optimize performance-critical paths.

3. **Component coordination failures:**
   ```
   Error: Summary→Index coordination failed
   ```
   **Solution:** Verify SSTable files are from the same generation and compatible.

### Debug Mode

Enable detailed logging for troubleshooting:

```rust
let config = GoldenPathConfig {
    detailed_metrics: true,
    ..Default::default()
};

// Set log level
env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("debug")).init();
```

### Validation Reports

Generate detailed validation reports:

```rust
// Generate comprehensive validation report
let validation_report = test_suite.generate_validation_report(&results);
println!("{}", validation_report);

// Export results for analysis
test_suite.export_results(&results, "golden_path_results.json").await?;
```

## Contributing

### Adding New Test Scenarios

1. Define scenario in `scenarios.rs`:
```rust
pub fn my_new_scenario() -> TestScenario {
    TestScenario {
        name: "my_new_test".to_string(),
        // ... configuration
    }
}
```

2. Add to scenario collections:
```rust
pub fn all_scenarios() -> Vec<TestScenario> {
    vec![
        // ... existing scenarios
        my_new_scenario(),
    ]
}
```

3. Add validation logic if needed in `validation.rs`

### Adding New Metrics

1. Extend `TestMetrics` struct in `mod.rs`
2. Update `MetricsCollector` in `metrics.rs`
3. Add validation logic in `validation.rs`

## License

This testing framework is part of the CQLite project and follows the same licensing terms.