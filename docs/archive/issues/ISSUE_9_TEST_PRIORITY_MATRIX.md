# Issue #9: Test Execution Priority Matrix

## 📊 Test Infrastructure Statistics

**Test Function Analysis:**
- Files with `#[test]` functions: 35 files
- Files with `#[tokio::test]` functions: 31 files  
- Total test functions identified: 136 tests
- Total Rust files in tests/: 115 files
- Test execution coverage: ~30% of files have test functions

## 🎯 Test Priority Classification

### 🔴 PRIORITY 1: CRITICAL CORE TESTS (Must Execute First)
**Target: 100% execution rate | Time Budget: <30 seconds**

#### Smoke Tests
- `tests/src/minimal_smoke_tests.rs` - Basic functionality validation
- `tests/src/lib.rs` basic integration test
- Core crate loading tests

#### Core Library Validation  
- `cqlite-core` unit tests
- Basic SSTable reading functionality
- Essential parser functionality

### 🟡 PRIORITY 2: INTEGRATION TESTS (High Value)
**Target: >95% execution rate | Time Budget: <2 minutes**

#### SSTable Format Tests
- `tests/src/sstable_format_tests.rs` - SSTable compatibility
- `tests/src/real_sstable_compatibility_test.rs` - Real file validation
- `tests/src/bti_validation.rs` - BTI format support (Cassandra 5+)

#### Parser Integration Tests
- `tests/src/cql_parser_validation_suite.rs` - CQL syntax validation
- `tests/src/parser_validation.rs` - Parser correctness
- `tests/src/comprehensive_parser_integration_tests.rs` - Comprehensive validation

#### Data Type Tests
- `tests/src/edge_case_data_types.rs` - Complex type handling
- `tests/src/complex_type_validation_suite.rs` - Type system validation
- Collection parsing tests

### 🟢 PRIORITY 3: PERFORMANCE & BENCHMARKS (Medium Value)
**Target: >80% execution rate | Time Budget: <2 minutes**

#### Performance Benchmarks
- `tests/benchmarks/performance_suite.rs` - Core performance
- `tests/benchmarks/end_to_end_bench.rs` - E2E performance
- `tests/src/cql_performance_benchmarks.rs` - CQL performance

#### Load Testing
- `tests/benchmarks/load_testing.rs` - Load validation
- `tests/src/performance_benchmarks.rs` - Performance regression

### 🔵 PRIORITY 4: EDGE CASES & SPECIALIZED (Lower Priority)
**Target: >75% execution rate | Time Budget: <1 minute**

#### Edge Case Testing
- `tests/src/edge_case_sstable_corruption.rs` - Corruption handling
- `tests/src/edge_case_stress_testing.rs` - Stress validation
- `tests/src/advanced_edge_case_tests.rs` - Advanced scenarios

#### Standalone Tests
- `tests/standalone/` - Standalone validation tools
- Format compatibility tests
- Docker integration tests

## 📈 Execution Strategy

### Phase 1: Smoke Test Validation (30 seconds)
```bash
# Execute minimal smoke tests first
cargo test minimal_smoke_tests --lib
cargo test integration_test_crate_loads --lib
```

### Phase 2: Core Functionality (2 minutes)
```bash
# Core integration tests
cargo test --test integration
cargo test sstable_format_tests
cargo test parser_validation
```

### Phase 3: Performance Validation (2 minutes)  
```bash
# Performance benchmarks (subset)
cargo bench --bench performance_suite
cargo test performance_benchmarks
```

### Phase 4: Comprehensive Suite (30 seconds)
```bash
# Edge cases and specialized tests
cargo test edge_case
cargo test standalone
```

## 🎯 Success Metrics by Priority

**Priority 1 (Critical):**
- Execution Rate: 100% (no failures allowed)
- Pass Rate: 100% 
- Time: <30 seconds

**Priority 2 (Integration):**
- Execution Rate: >95%
- Pass Rate: >90%
- Time: <2 minutes

**Priority 3 (Performance):**
- Execution Rate: >80%
- Pass Rate: >85%
- Time: <2 minutes

**Priority 4 (Edge Cases):**
- Execution Rate: >75%
- Pass Rate: >80%
- Time: <1 minute

**OVERALL TARGET: >80% total execution rate in <5 minutes**

## 🔧 Test Execution Commands

### Quick Health Check (30 seconds)
```bash
cargo check --tests
cargo test --lib --no-run  # Compile only
cargo test minimal_smoke_tests
```

### Core Validation Suite (2 minutes)
```bash
cargo test --test '*integration*'
cargo test sstable_format_tests
cargo test parser_validation  
cargo test bti_validation
```

### Performance Baseline (2 minutes)
```bash
cargo bench --bench performance_suite -- --test
cargo test performance_benchmarks --release
cargo test cql_performance_benchmarks
```

### Full Suite (5 minutes total)
```bash
cargo test --workspace --release
cargo bench --workspace -- --test
```

## ⚠️ Known Risk Areas

**High Risk Tests (May Fail):**
- Docker integration tests (environment dependent)
- Real Cassandra data tests (data file dependent)  
- Network-dependent tests
- File system permission tests

**Medium Risk Tests:**
- Performance benchmarks (hardware dependent)
- Memory-intensive tests
- Complex type validation

**Low Risk Tests:**
- Unit tests
- Smoke tests
- Basic parser tests

## 📊 Coverage Analysis Framework

**Test Categories:**
1. **Unit Tests**: 40 tests (Priority 1)
2. **Integration Tests**: 50 tests (Priority 2)  
3. **Performance Tests**: 25 tests (Priority 3)
4. **Edge Case Tests**: 21 tests (Priority 4)

**Total Test Budget**: 136 tests across 4 priorities
**Target Execution**: >109 tests (80% of 136)
**Target Time**: <5 minutes total

## 🚨 Blocking Issues

**CRITICAL BLOCKER:**
- Issue #8 compilation errors MUST be fixed first
- Cannot execute ANY tests until clean compilation

**Ready for Immediate Execution:**
- Test priority matrix complete
- Execution commands prepared
- Success metrics defined
- Coverage framework ready

---

**Status**: READY - Waiting for Issue #8 compilation fix
**Next Action**: Execute Phase 1 smoke tests immediately upon clean compilation