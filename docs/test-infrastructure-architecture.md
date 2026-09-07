# Test Infrastructure Architecture - Phase 2

## Overview

The Enhanced TestContext Framework is designed to achieve tiered test coverage for CQLite (see [PRD Section 5.1](development/PRD.md#51--tiered-coverage-targets)) by providing:

1. **Enhanced TestContext**: Schema validation, coverage tracking, quality gates
2. **Test Categorization**: Systematic organization for comprehensive coverage
3. **Quality Gate Framework**: Automated validation of coverage and performance targets
4. **Property-Based Testing**: Advanced test generation and validation

## Architecture Components

### 1. Test Categories

```rust
pub enum TestCategory {
    Unit(UnitSubcategory),           // Individual component tests
    Integration(IntegrationSubcategory), // Component interaction tests
    Performance(PerformanceSubcategory), // Performance and regression tests
    Property(PropertySubcategory),   // Property-based testing
    EndToEnd(E2ESubcategory),       // System-level tests
}
```

**Coverage Targets by Category:**
- Unit Tests: Tiered by module criticality (Critical 90%+, Important 80%+, Supporting 70%+, Utilities 50%+)
- Integration Tests: 80% path coverage for cross-module interactions
- Performance Tests: Baseline + regression detection
- Property Tests: 1000+ generated test cases
- End-to-End Tests: Critical user scenarios

See [PRD Section 5.1](development/PRD.md#51--tiered-coverage-targets) for authoritative tier definitions.

### 2. Enhanced TestContext

```rust
// Builder pattern for flexible configuration
let context = EnhancedTestContext::builder()
    .category(TestCategory::Integration(IntegrationSubcategory::SSTableReading))
    .schema_validation(true)
    .coverage_tracking(true)
    .quality_gate(QualityGate::new().min_coverage(90.0))
    .build("test_basic")
    .await?;
```

**Key Features:**
- Real-time coverage tracking
- Schema validation against expected schemas
- Property test configuration
- Quality gate enforcement
- Memory and performance monitoring

### 3. Quality Gates

Quality gates enforce minimum standards:

```rust
pub struct QualityGate {
    pub min_coverage: f64,           // 75% overall, tiered by module
    pub min_branch_coverage: f64,    // 60-80% depending on tier
    pub max_execution_time: Duration, // Performance limits
    pub max_memory_usage: usize,     // Memory limits
    pub component_targets: HashMap<String, f64>, // Per-component tiered targets
}
```

**Enforcement Points:**
- Pre-commit hooks
- CI/CD pipeline gates
- Local development validation
- Release candidate verification

### 4. Coverage Tracking

Real-time coverage analysis:

```rust
pub struct CoverageTracker {
    pub lines_covered: HashSet<String>,
    pub branches_covered: HashMap<String, BranchCoverage>,
    pub functions_covered: HashSet<String>,
    pub component_coverage: HashMap<String, ComponentCoverage>,
}
```

**Component Coverage Targets (aligned with PRD tiered approach):**
- `parser`, `storage::sstable::reader`, `storage::sstable::reader::parsing`: 90% (Critical tier)
- `query`, `schema`, `types`, `cql`, `discovery`: 80% (Important tier)
- `memory`, `platform`, `storage::sstable::directory`, `storage::sstable::bti`: 70% (Supporting tier)
- `benchmarks`, `testing`: 50% (Utilities tier)

## Test Organization Strategy

### Phase 2 Test Enablement Plan

**Priority 1: Core SSTable Operations (15 tests)**
- `test_sstable_component_discovery_comprehensive`
- `test_sstable_reader_memory_decompression_advanced`
- `test_index_db_parsing_with_real_offsets`
- `test_partition_lookup_performance_regression`
- `test_concurrent_sstable_access_stress`

**Priority 2: Schema and Validation (12 tests)**
- `test_schema_driven_parsing_comprehensive`
- `test_sstabledump_parity_validation_strict`
- `test_compression_pipeline_integrity`
- `test_format_version_compatibility_matrix`

**Priority 3: Edge Cases and Error Handling (8 tests)**
- `test_malformed_sstable_error_recovery`
- `test_memory_pressure_graceful_degradation`
- `test_concurrent_read_write_consistency`

**Priority 4: Performance and Regression (8 tests)**
- `test_throughput_baseline_validation`
- `test_latency_percentile_tracking`
- `test_memory_usage_profiling`

## Agent Coordination Schema

### Memory Keys for Multi-Agent Coordination

```
swarm/test-infrastructure/
├── framework-design          # Enhanced TestContext architecture
├── category-mapping          # Test category assignments
├── coverage-targets          # Per-component coverage goals
├── quality-gates             # Quality gate configurations
├── ignored-tests             # 43 tests to be enabled
├── test-priorities           # Enablement priority order
└── coordination-protocol     # Agent communication patterns
```

### Agent Roles and Responsibilities

1. **Test Infrastructure Architect** (This Agent)
   - Framework design and implementation
   - Quality gate definition
   - Agent coordination

2. **Test Enablement Specialist**
   - Convert ignored tests to real data tests
   - Implement missing test scenarios
   - Validate test correctness

3. **Coverage Analysis Agent**
   - Monitor coverage metrics
   - Identify coverage gaps
   - Report progress to quality gates

4. **Performance Validation Agent**
   - Implement performance regression tests
   - Establish baseline metrics
   - Monitor performance trends

5. **Schema Validation Agent**
   - Implement schema-driven test validation
   - Create schema evolution tests
   - Validate backwards compatibility

## Implementation Guidelines

### For Test Authors

```rust
#[tokio::test]
async fn test_sstable_comprehensive_validation() {
    let mut context = EnhancedTestContext::builder()
        .category(TestCategory::Integration(IntegrationSubcategory::SSTableReading))
        .schema_validation(true)
        .coverage_tracking(true)
        .quality_gate(
            QualityGate::new()
                .min_coverage(90.0)
                .component_target("storage::sstable".to_string(), 95.0)
        )
        .build("test_basic")
        .await?;

    // Test implementation with automatic coverage tracking
    context.run_test_with_coverage(|| async {
        // Your test logic here
        context.record_coverage("sstable_reader.rs", 123);

        // Schema validation
        let schema = load_test_schema();
        let validation = context.validate_schema(&schema)?;
        assert!(validation.valid);

        Ok(())
    }).await?;

    // Automatic quality gate validation
    let metrics = context.validate_and_cleanup().await?;
    println!("Coverage: {:.2}%", metrics.coverage.coverage_percentage());
}
```

### Feature Flags

Enable enhanced testing features:

```toml
[features]
test-infrastructure = []
```

Its four former leaves (`test-schema-validation`, `test-property-testing`,
`test-coverage-tracking`, `test-quality-gates`) were deleted in #1698 — none of them
had a single `cfg` site, so enabling any of them changed nothing. `test-infrastructure`
itself has a real site (`cqlite-core/tests/common/mod.rs`) and stays.

## Success Metrics

### Phase 2 Targets

- **Overall Coverage**: 75% aggregate (weighted by module size)
- **Tiered Coverage**: Critical 90%+, Important 80%+, Supporting 70%+, Utilities 50%+
- **Test Enablement**: All 43 ignored tests converted and passing
- **Quality Gates**: Zero tolerance for coverage regressions
- **Performance**: No regressions > 5% from baseline
- **Memory**: Maximum 512MB usage during test execution

### Monitoring and Reporting

- Real-time coverage dashboards
- Automated coverage regression detection
- Performance trend analysis
- Quality gate violation alerts
- Component-level coverage heatmaps

## Integration with CI/CD

### Pre-commit Hooks
- Coverage validation
- Quality gate checks
- Performance regression detection

### CI Pipeline Gates
- Minimum coverage enforcement
- Component-specific validation
- Integration test matrix execution

### Release Validation
- Full test suite execution
- Coverage report generation
- Performance baseline validation
- Backwards compatibility verification