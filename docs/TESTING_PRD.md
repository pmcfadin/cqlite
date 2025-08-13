# CQLite Testing Strategy - Product Requirements Document (PRD)

## Executive Summary

This PRD defines a comprehensive testing strategy for CQLite that emphasizes **simplicity, repeatability, and isolation** through Docker-based single-node testing with real Cassandra data generation.

### Core Principles
- **Docker single-node approach** for predictable, fast testing
- **Ephemeral containers** that reset cleanly for each test cycle
- **Real data integration** with actual Cassandra-generated SSTables
- **Human-executable verification** with simple command-line scripts
- **CI/CD friendly** with GitHub Actions integration
- **Repeatable and isolated** testing across all environments

## 1. Requirements

### 1.1 Functional Requirements

#### 1.1.1 Test Data Generation
- **REQ-1.1**: Generate authentic SSTable files using real Cassandra containers
- **REQ-1.2**: Support all CQL data types (primitives, collections, UDTs, counters)
- **REQ-1.3**: Create test data for multiple scenarios (basic types, time-series, large tables, wide rows)
- **REQ-1.4**: Export generated data to local directories for CQLite testing
- **REQ-1.5**: Generate test data with configurable volume (small, medium, large datasets)

#### 1.1.2 Container Management
- **REQ-2.1**: Use single Cassandra node per test scenario (not clusters)
- **REQ-2.2**: Support configurable Cassandra versions (5.0 primary, 4.x/3.x optional)
- **REQ-2.3**: Provide complete container reset capability (ephemeral approach)
- **REQ-2.4**: Implement proper health checks and startup validation
- **REQ-2.5**: Isolate test environments with dedicated Docker networks

#### 1.1.3 Human Usability
- **REQ-3.1**: Provide single-command test execution (`./run-tests.sh`)
- **REQ-3.2**: Include simple verification script for manual testing
- **REQ-3.3**: Generate human-readable test reports and summaries
- **REQ-3.4**: Provide debugging tools for troubleshooting test failures
- **REQ-3.5**: Include clear error messages and resolution guidance

#### 1.1.4 CI/CD Integration
- **REQ-4.1**: Integration with GitHub Actions workflows
- **REQ-4.2**: Automated test execution on pull requests
- **REQ-4.3**: Quality gates with specific pass/fail criteria
- **REQ-4.4**: Artifact preservation for test results and generated data
- **REQ-4.5**: Performance regression detection and reporting

### 1.2 Non-Functional Requirements

#### 1.2.1 Performance
- **NFR-1.1**: Test data generation completes in <10 minutes for standard scenarios
- **NFR-1.2**: Container startup time <60 seconds
- **NFR-1.3**: Data export operations complete in <30 seconds
- **NFR-1.4**: Memory usage <2GB for single-node testing
- **NFR-1.5**: Disk usage <1GB for exported test data

#### 1.2.2 Reliability
- **NFR-2.1**: 99% repeatability - same data generation across environments
- **NFR-2.2**: 100% isolation - no test interference between runs
- **NFR-2.3**: Graceful failure handling with clear error reporting
- **NFR-2.4**: Automatic cleanup on test completion or failure
- **NFR-2.5**: Cross-platform compatibility (Linux, macOS, Windows with Docker)

#### 1.2.3 Maintainability
- **NFR-3.1**: Simple configuration with YAML/TOML files
- **NFR-3.2**: Modular design for easy addition of new test scenarios
- **NFR-3.3**: Clear documentation with examples and troubleshooting
- **NFR-3.4**: Self-contained setup with minimal external dependencies
- **NFR-3.5**: Version-controlled test schemas and generation scripts

## 2. Architecture

### 2.1 Overall System Design

```
┌─────────────────────────────────────────────────────────────┐
│                  CQLite Testing Framework                   │
├─────────────────────────────────────────────────────────────┤
│  Test Orchestrator (run-tests.sh)                          │
│  ├── Container Manager (Docker Compose)                    │
│  ├── Data Generator (Schema + Population Scripts)          │
│  ├── Data Exporter (SSTable Extraction)                    │
│  └── Validator (CQLite Integration Tests)                  │
├─────────────────────────────────────────────────────────────┤
│  Infrastructure Layer                                       │
│  ├── Docker Engine                                         │
│  ├── Single Cassandra Container (Ephemeral)                │
│  ├── Volume Mounts (Local Directory Export)                │
│  └── Isolated Networks (Clean Environment)                 │
├─────────────────────────────────────────────────────────────┤
│  Data Output                                                │
│  ├── ./test-data/sstables/ (Exported SSTable files)        │
│  ├── ./test-data/schemas/ (CQL schema definitions)         │
│  ├── ./test-data/metadata/ (Test configuration & results)  │
│  └── ./test-data/reports/ (Validation and test reports)    │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Component Architecture

#### 2.2.1 Test Orchestrator
```bash
run-tests.sh
├── Validates Docker installation
├── Loads test configuration
├── Manages container lifecycle
├── Coordinates data generation and export
├── Runs validation tests
└── Generates reports
```

#### 2.2.2 Container Management
```yaml
# docker-compose.yml
services:
  cassandra:
    image: cassandra:${CASSANDRA_VERSION:-5.0}
    container_name: cqlite-test-cassandra
    environment:
      - CASSANDRA_CLUSTER_NAME=CQLiteTest
    volumes:
      - ./test-data/export:/opt/export
    networks:
      - cqlite-test-network
    healthcheck:
      test: ["CMD", "cqlsh", "-e", "describe keyspaces"]
      interval: 30s
      timeout: 10s
      retries: 10
```

#### 2.2.3 Data Generation Pipeline
1. **Schema Creation**: Load CQL schema definitions
2. **Data Population**: Execute population scripts with seeded data
3. **SSTable Generation**: Allow Cassandra to create SSTable files
4. **Data Export**: Copy SSTable files to local directories
5. **Metadata Creation**: Generate test metadata and configuration files

### 2.3 Data Export Structure
```
test-data/
├── sstables/
│   ├── basic_types/
│   │   ├── all_types-{uuid}/
│   │   │   ├── nb-1-big-Data.db
│   │   │   ├── nb-1-big-Index.db
│   │   │   └── [other SSTable components]
│   ├── collections/
│   ├── time_series/
│   └── wide_rows/
├── schemas/
│   ├── basic_types.cql
│   ├── collections.cql
│   └── time_series.cql
├── metadata/
│   ├── test_config.yaml
│   ├── generation_report.json
│   └── validation_results.json
└── reports/
    ├── summary.html
    └── detailed_results.json
```

## 3. Implementation Strategy

### 3.1 Foundation: test-env/cassandra5/ Enhancement

**Based on team analysis, `test-env/cassandra5/` is 95% aligned with requirements.**

#### 3.1.1 Current Strengths (Keep These)
- ✅ Single-node Docker approach
- ✅ Ephemeral container management (`manage.sh reset`)
- ✅ Real SSTable data generation
- ✅ Local directory export (`extract-sstables`)
- ✅ Human-executable scripts (`manage.sh all`)
- ✅ Comprehensive data types and scenarios

#### 3.1.2 Required Enhancements
1. **Add CI/CD Integration Script**:
   ```bash
   # ci-verify.sh
   #!/bin/bash
   set -e
   ./manage.sh all
   ./manage.sh extract-sstables
   ./validate-export.sh
   echo "✅ CI verification complete"
   ```

2. **Version Configuration Support**:
   ```bash
   # Support CASSANDRA_VERSION environment variable
   export CASSANDRA_VERSION=${CASSANDRA_VERSION:-5.0}
   ```

3. **Enhanced Validation**:
   ```bash
   # validate-export.sh
   #!/bin/bash
   # Verify SSTable files exist and are readable
   # Check file sizes and structure
   # Validate CQLite can parse generated files
   ```

### 3.2 Implementation Phases

#### Phase 1: Foundation (Weeks 1-2)
- **Week 1**: Enhance test-env/cassandra5/ with CI integration
- **Week 2**: Add comprehensive validation and reporting

**Deliverables:**
- Enhanced `manage.sh` with CI support
- Automated validation scripts
- Basic GitHub Actions integration

#### Phase 2: Comprehensive Testing (Weeks 3-4)
- **Week 3**: Add multiple test scenarios and data varieties
- **Week 4**: Performance testing and optimization

**Deliverables:**
- Multiple test scenarios (5+ different data patterns)
- Performance benchmarks and regression detection
- Comprehensive test report generation

#### Phase 3: Human Tools (Weeks 5-6)
- **Week 5**: Human verification tools and debugging utilities
- **Week 6**: Documentation and troubleshooting guides

**Deliverables:**
- Debug and troubleshooting tools
- Human-friendly verification scripts
- Complete documentation and examples

### 3.3 Test Scenarios

#### 3.3.1 Core Test Scenarios
1. **Basic Data Types**: All CQL primitives, nulls, edge cases
2. **Collections**: Sets, lists, maps, nested structures
3. **Time Series**: Time-based partitioning, TTLs, clustering
4. **Wide Rows**: Many columns, large partitions, sparse data
5. **User-Defined Types**: UDTs, frozen types, nested UDTs
6. **Counters**: Counter columns and operations
7. **Large Tables**: Performance testing with substantial data volumes

#### 3.3.2 Test Scenario Configuration
```yaml
# test-scenarios.yaml
scenarios:
  basic_types:
    name: "Basic Data Types"
    schema: "schemas/basic_types.cql"
    row_count: 10000
    validation: "validate_basic_types"
    
  collections:
    name: "Collection Types"
    schema: "schemas/collections.cql"
    row_count: 5000
    validation: "validate_collections"
    
  time_series:
    name: "Time Series Data"
    schema: "schemas/time_series.cql"
    row_count: 50000
    validation: "validate_time_series"
```

## 4. CI/CD Integration

### 4.1 GitHub Actions Workflow

```yaml
name: CQLite Testing Framework

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test-data-generation:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v4
    
    - name: Set up Docker
      uses: docker/setup-buildx-action@v3
      
    - name: Generate Test Data
      run: |
        cd test-env/cassandra5
        ./ci-verify.sh
        
    - name: Run CQLite Integration Tests
      run: |
        cargo test --release -- --include-ignored
        
    - name: Upload Test Artifacts
      uses: actions/upload-artifact@v4
      with:
        name: test-data-results
        path: test-env/cassandra5/test-data/
        retention-days: 30
        
    - name: Generate Test Report
      run: |
        ./generate-test-report.sh > test-summary.md
        
    - name: Comment PR with Results
      if: github.event_name == 'pull_request'
      uses: actions/github-script@v7
      with:
        script: |
          const fs = require('fs');
          const report = fs.readFileSync('test-summary.md', 'utf8');
          github.rest.issues.createComment({
            issue_number: context.issue.number,
            owner: context.repo.owner,
            repo: context.repo.repo,
            body: report
          });
```

### 4.2 Quality Gates

#### 4.2.1 Success Criteria
- ✅ All test scenarios complete successfully
- ✅ SSTable files generated and exported
- ✅ CQLite can parse all generated files
- ✅ No memory leaks or resource issues
- ✅ Performance within acceptable thresholds

#### 4.2.2 Performance Thresholds
- **Data Generation**: <10 minutes for all scenarios
- **Container Startup**: <60 seconds
- **SSTable Export**: <30 seconds
- **Memory Usage**: <2GB peak usage
- **Disk Usage**: <1GB for exported data

## 5. Human Usage

### 5.1 Simple Commands

#### 5.1.1 Full Test Suite
```bash
cd test-env/cassandra5
./run-tests.sh
```

#### 5.1.2 Single Test Scenario
```bash
./run-single-test.sh basic_types
```

#### 5.1.3 Debug Mode
```bash
./debug-parser.sh test-data/sstables/basic_types/
```

#### 5.1.4 Clean Reset
```bash
./manage.sh reset
```

### 5.2 Verification Checklist

#### 5.2.1 Manual Verification Steps
1. **Container Health**: `docker ps` shows healthy Cassandra container
2. **Data Generation**: Check logs for successful schema and data creation
3. **File Export**: Verify SSTable files exist in `test-data/sstables/`
4. **CQLite Integration**: Run `cqlite info` on generated files
5. **Performance**: Check generation time within thresholds

#### 5.2.2 Troubleshooting Tools
```bash
# Debug container issues
./debug-container.sh

# Check SSTable file structure
./inspect-sstables.sh [path]

# Validate CQLite parsing
./test-cqlite-parsing.sh [sstable-file]

# Performance profiling
./profile-generation.sh
```

## 6. Data Strategy

### 6.1 Real Data Philosophy

#### 6.1.1 Integration & E2E Testing
- **Use Real Data**: Authentic Cassandra-generated SSTables
- **Comprehensive Coverage**: All CQL data types and structures
- **Realistic Scenarios**: Production-like data patterns and volumes
- **Edge Case Testing**: Boundary conditions, large values, nulls

#### 6.1.2 Unit Testing
- **Strategic Mocking**: Mock external dependencies where appropriate
- **Fast Execution**: Unit tests complete in seconds, not minutes
- **Isolated Testing**: Test individual components without Docker overhead

### 6.2 Data Generation Strategy

#### 6.2.1 Seeded Generation
```python
# Deterministic data generation
FAKER_SEED = 1234
random.seed(FAKER_SEED)
fake = Faker()
Faker.seed(FAKER_SEED)
```

#### 6.2.2 Configurable Volumes
```yaml
data_volumes:
  small: 1000      # For development and quick tests
  medium: 10000    # For comprehensive testing
  large: 100000    # For performance testing
  xlarge: 1000000  # For stress testing
```

#### 6.2.3 Data Variety
- **Realistic Data**: Names, addresses, timestamps, UUIDs
- **Edge Cases**: Empty strings, nulls, maximum values
- **Unicode Support**: International characters and symbols
- **Binary Data**: BLOBs and large text fields

## 7. Success Criteria

### 7.1 Technical Success Metrics

#### 7.1.1 Reliability
- **99%+ repeatability** across different environments
- **100% isolation** between test runs
- **Zero data contamination** between scenarios
- **Graceful failure handling** with clear error messages

#### 7.1.2 Performance
- **<10 minutes** for complete test suite execution
- **<2GB** peak memory usage
- **<1GB** exported data size
- **<60 seconds** container startup time

#### 7.1.3 Coverage
- **100% CQL data type coverage** in test scenarios
- **Multiple compression formats** (LZ4, Snappy, None)
- **Various table structures** (narrow, wide, time-series)
- **Edge cases and boundary conditions**

### 7.2 Usability Success Metrics

#### 7.2.1 Human Usability
- **Single command execution** for full test suite
- **Clear output and progress indication**
- **Helpful error messages with resolution guidance**
- **Self-contained setup** with minimal dependencies

#### 7.2.2 Developer Experience
- **<5 minutes** setup time for new developers
- **Clear documentation** with examples
- **Easy debugging** with inspection tools
- **Consistent results** across development environments

### 7.3 Integration Success Metrics

#### 7.3.1 CI/CD Integration
- **Automated execution** on all pull requests
- **Quality gates** with pass/fail criteria
- **Artifact preservation** for debugging
- **Performance regression detection**

#### 7.3.2 CQLite Integration
- **100% parsing success** for generated SSTable files
- **Comprehensive test coverage** of CQLite functionality
- **Performance validation** of parsing operations
- **Real-world scenario testing**

## 8. Risk Assessment & Mitigation

### 8.1 Technical Risks

#### 8.1.1 Container Dependencies
- **Risk**: Docker or Cassandra version compatibility issues
- **Mitigation**: Version pinning, compatibility testing, fallback versions

#### 8.1.2 Data Generation Consistency
- **Risk**: Non-deterministic data generation across environments
- **Mitigation**: Seeded random generation, checksums, validation scripts

#### 8.1.3 Performance Degradation
- **Risk**: Test execution time increases over time
- **Mitigation**: Performance monitoring, benchmarks, optimization alerts

### 8.2 Operational Risks

#### 8.2.1 CI/CD Resource Usage
- **Risk**: Excessive CI resources for Docker-based testing
- **Mitigation**: Efficient container management, caching, parallel execution

#### 8.2.2 Maintenance Overhead
- **Risk**: Complex test infrastructure becomes difficult to maintain
- **Mitigation**: Simple design, clear documentation, automated validation

## 9. Implementation Timeline

### 9.1 Phase 1: Foundation (Weeks 1-2)
- **Week 1**: 
  - Enhance test-env/cassandra5/ with CI integration
  - Create ci-verify.sh script
  - Add version configuration support
  
- **Week 2**:
  - Implement comprehensive validation scripts
  - Create basic GitHub Actions workflow
  - Add automated test reporting

### 9.2 Phase 2: Comprehensive Testing (Weeks 3-4)
- **Week 3**:
  - Add multiple test scenarios configuration
  - Implement performance benchmarking
  - Create test scenario management
  
- **Week 4**:
  - Optimize performance and resource usage
  - Add regression detection
  - Enhance error handling and reporting

### 9.3 Phase 3: Human Tools & Documentation (Weeks 5-6)
- **Week 5**:
  - Create debugging and inspection tools
  - Implement human verification scripts
  - Add troubleshooting utilities
  
- **Week 6**:
  - Complete documentation and examples
  - Create troubleshooting guides
  - Final testing and validation

## 10. Conclusion

This testing strategy provides a **simple, repeatable, and reliable** approach to test data generation for CQLite using Docker-based single-node Cassandra containers. The strategy emphasizes:

- **Simplicity**: Single-command execution and clear workflows
- **Repeatability**: Deterministic data generation and clean resets
- **Isolation**: Ephemeral containers and dedicated environments
- **Real Data**: Authentic Cassandra-generated SSTables for integration testing
- **Human Usability**: Easy verification and debugging tools
- **CI/CD Integration**: Automated testing with quality gates

By building on the existing `test-env/cassandra5/` foundation, which already meets 95% of the requirements, this approach provides maximum value with minimal risk and complexity.