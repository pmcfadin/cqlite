# CQLite E2E Testing Infrastructure with Real Cassandra 5+ Validation

This directory contains a comprehensive end-to-end testing infrastructure that validates CQLite's compatibility with real Cassandra 5+ clusters. It provides automated testing, performance benchmarking, and continuous validation capabilities.

## 🎯 Testing Objectives

1. **Complete Cassandra 5+ Compatibility**: Validate round-trip data fidelity with real Cassandra clusters
2. **Performance Validation**: Ensure CQLite meets or exceeds performance targets 
3. **Real-World Data Testing**: Test with production-scale datasets and usage patterns
4. **Continuous Regression Testing**: Automated CI/CD pipeline for compatibility validation
5. **Stress Testing**: Validate behavior under high load and concurrent operations

## 🏗️ Infrastructure Components

### Docker Cluster Setup
- **3-node Cassandra 5.0 cluster** with proper replication
- **Automated test data generation** with comprehensive type coverage
- **CQLite validation containers** for round-trip testing
- **Performance benchmarking services** for regression detection

### Test Categories
1. **Primitive Type Compatibility** - All CQL data types with edge cases
2. **Collection Type Testing** - Lists, sets, maps, and nested structures
3. **User Defined Types** - Complex nested UDT validation
4. **Composite Key Testing** - Multi-part primary keys and clustering
5. **Large Dataset Processing** - 1GB+ SSTable file handling
6. **Concurrent Operations** - Multi-threaded safety and consistency
7. **Edge Case Handling** - Unicode, null values, max/min boundaries
8. **Performance Benchmarking** - Throughput and latency validation

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- At least 8GB RAM available for containers
- Python 3.8+ with cassandra-driver for dataset generation

### Basic Usage

1. **Start the testing cluster:**
```bash
cd tests/cassandra-cluster
docker-compose up -d
```

2. **Run comprehensive validation:**
```bash
# Wait for cluster to be ready (2-3 minutes)
docker-compose up --abort-on-container-exit cqlite-e2e-validator
```

3. **Run performance benchmarks:**
```bash
docker-compose up --abort-on-container-exit cqlite-performance-tester
```

4. **View results:**
```bash
# Check validation results
docker-compose exec cqlite-e2e-validator cat /opt/test-data/validation-results/comprehensive-validation-report.json

# Check performance results  
docker-compose exec cqlite-performance-tester cat /opt/test-data/performance-results/performance-regression-analysis.json
```

### CI/CD Pipeline

Run the complete automated pipeline:
```bash
cd tests/cassandra-cluster
./scripts/ci-cd-pipeline.sh
```

This will:
- Set up a 3-node Cassandra 5+ cluster
- Generate comprehensive test datasets
- Run all compatibility and performance tests
- Generate detailed reports with pass/fail status
- Clean up resources automatically

## 📊 Test Data Generation

### Comprehensive Test Datasets

The infrastructure generates multiple categories of test data:

1. **All Primitive Types** (1,000 records)
   - Complete coverage of CQL primitive types
   - Edge cases: null values, Unicode, max/min values
   - Different compression algorithms

2. **Collection Types** (500 records)
   - Lists, sets, maps with various data types
   - Nested collections and frozen variants
   - Complex nested structures

3. **User Defined Types** (200 records)
   - Multi-level nested UDTs
   - UDTs within collections
   - Real-world patterns (addresses, profiles)

4. **Time Series Data** (5,000 records)
   - IoT sensor simulation with realistic patterns
   - Time-based partitioning
   - High-frequency data ingestion patterns

5. **Large Dataset Test** (100 records, 100MB total)
   - 1MB+ individual records
   - Binary data with various patterns
   - Memory efficiency validation

### Real-World Production Datasets

Generate production-scale datasets:
```bash
# Inside Cassandra container
python3 /opt/real-world-data/generate-production-datasets.py \
    --iot-devices 1000 \
    --iot-days 30 \
    --users 10000 \
    --content 5000
```

This creates:
- **IoT sensor data**: Time series with realistic sensor patterns
- **User profiles**: Complex nested user data with social profiles
- **Content management**: Full-text content with metadata
- **Analytics events**: High-volume event tracking data

## 🔄 Round-Trip Validation Process

The round-trip validation ensures complete compatibility:

1. **Cassandra → CQLite**: 
   - Parse real Cassandra 5+ SSTable files
   - Validate all data types and structures
   - Verify metadata preservation

2. **CQLite → Cassandra**:
   - Generate SSTable files from CQLite
   - Import back into Cassandra
   - Verify data integrity and queryability

3. **Bidirectional Consistency**:
   - Compare original vs round-trip data
   - Validate checksums and compression
   - Ensure zero data loss

## ⚡ Performance Benchmarking

### Benchmark Categories

1. **SSTable Parsing Performance**
   - Throughput: >10MB/s for typical datasets
   - Latency: <100ms for 1MB files
   - Memory efficiency: <100MB for 100MB dataset

2. **Concurrent Operations**
   - Multi-threaded safety validation
   - Throughput under concurrent load
   - Memory usage under stress

3. **Large Dataset Handling**
   - 1GB+ SSTable file processing
   - Memory-mapped file efficiency
   - Streaming vs full-load performance

4. **Type Serialization Performance**
   - Primitive type serialization speed
   - Collection type overhead
   - Complex nested structure performance

### Performance Targets

- **Parse Speed**: >10MB/s sustained throughput
- **Memory Usage**: <2x of data size in memory
- **Concurrent Throughput**: >1000 ops/sec with 8 threads
- **Large File Handling**: Process 1GB+ files without memory issues

## 🧪 Test Execution Modes

### Local Development Mode
```bash
# Start cluster and keep running for development
docker-compose up -d

# Run specific test categories
cargo test --package cqlite-integration-tests test_cassandra5_sstable_compatibility
cargo test --package cqlite-integration-tests test_large_dataset_processing
cargo test --package cqlite-integration-tests test_concurrent_round_trip_operations
```

### CI/CD Mode
```bash
# Automated pipeline with cleanup
export CI=true
./scripts/ci-cd-pipeline.sh
```

### Performance Testing Mode
```bash
# Extended performance validation
export PERFORMANCE_MODE=extended
docker-compose up --abort-on-container-exit cqlite-performance-tester
```

### Stress Testing Mode
```bash
# High-load stress testing
export STRESS_TEST_ENABLED=true
export MAX_TEST_DATA_SIZE=1GB
docker-compose up --abort-on-container-exit cqlite-e2e-validator
```

## 📈 Results and Reporting

### Validation Reports

All tests generate comprehensive JSON reports:

```json
{
  "validation_report": {
    "timestamp": "2025-01-21T12:00:00Z",
    "overall_status": "PASS",
    "compatibility_score": 0.998,
    "summary": {
      "total_tests": 15,
      "passed_tests": 15,
      "failed_tests": 0,
      "performance_score": 0.95
    },
    "detailed_results": {
      "primitive_types": {"status": "PASS", "score": 1.0},
      "collection_types": {"status": "PASS", "score": 0.99},
      "large_datasets": {"status": "PASS", "score": 0.96}
    }
  }
}
```

### Performance Reports

Performance benchmarks include:
- Throughput measurements (ops/sec, MB/s)
- Latency percentiles (p50, p95, p99)
- Memory usage profiles
- Regression analysis vs baselines

### CI/CD Integration

The pipeline generates artifacts for:
- Test result summaries
- Performance regression analysis
- Compatibility score tracking
- Failure root cause analysis

## 🔧 Configuration

### Environment Variables

```bash
# Cassandra cluster configuration
CASSANDRA_CONTACT_POINTS=cassandra5-seed,cassandra5-node2,cassandra5-node3
CASSANDRA_PORT=9042
CASSANDRA_TIMEOUT=600

# Testing configuration
TEST_MODE=COMPREHENSIVE
PERFORMANCE_THRESHOLD=0.95
COMPATIBILITY_THRESHOLD=0.98
MAX_TEST_DATA_SIZE=1GB

# CI/CD configuration
CI=false
RUST_LOG=info
RUST_BACKTRACE=1
```

### Customizing Test Data

Modify `scripts/generate-comprehensive-test-data.sh` to:
- Adjust record counts for different test scenarios
- Add custom data patterns
- Configure compression algorithms
- Set replication factors

## 🚨 Troubleshooting

### Common Issues

1. **Cluster startup timeout**:
   ```bash
   # Increase timeout and check logs
   export CASSANDRA_TIMEOUT=900
   docker-compose logs cassandra5-seed
   ```

2. **Memory issues**:
   ```bash
   # Reduce test data size
   export MAX_TEST_DATA_SIZE=100MB
   docker-compose up --scale cassandra5-node2=0 --scale cassandra5-node3=0
   ```

3. **Performance test failures**:
   ```bash
   # Check system resources
   free -h
   docker stats
   # Adjust performance thresholds
   export PERFORMANCE_THRESHOLD=0.8
   ```

### Debug Mode

Enable verbose logging:
```bash
export RUST_LOG=debug
export CASSANDRA_DEBUG=true
docker-compose up
```

## 📚 Advanced Usage

### Custom Test Scenarios

Create custom test scenarios by:
1. Adding new test data patterns in `generate-comprehensive-test-data.sh`
2. Implementing new test cases in `tests/src/integration_e2e.rs`
3. Adding performance benchmarks in `benchmarks/`

### Integration with External Systems

The infrastructure supports:
- Custom Cassandra configurations
- External monitoring systems
- Custom performance baselines
- Integration with existing CI/CD pipelines

## 🎯 Quality Gates

The testing infrastructure enforces these quality gates:

1. **Compatibility**: ≥98% round-trip data fidelity
2. **Performance**: ≥95% of baseline performance
3. **Memory**: ≤2x data size memory usage  
4. **Concurrency**: Safe multi-threaded operations
5. **Reliability**: Zero data corruption under stress

## 🤝 Contributing

To add new test scenarios:

1. Add test data generation in `scripts/generate-comprehensive-test-data.sh`
2. Implement test logic in `tests/src/integration_e2e.rs`
3. Add performance benchmarks if needed
4. Update documentation and expected baselines
5. Test with the full CI/CD pipeline

---

This comprehensive testing infrastructure ensures CQLite maintains 100% compatibility with Cassandra 5+ while meeting all performance and reliability requirements.