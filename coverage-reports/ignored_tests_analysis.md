# Ignored Tests Analysis - cqlite Phase 2
*Generated: 2025-09-27*

## Summary
- **Total ignored test cases**: 15
- **Files affected**: 9
- **Target**: 0 ignored tests for M1 milestone
- **Priority**: Convert high-impact tests first

## Ignored Test Locations

Based on current analysis, here are the 15 ignored tests that need resolution:

### Integration Tests (High Priority)
1. `./tests/integration/test_sstabledump_parity_integration.rs:274` - Integration test requiring Cassandra tools
2. `./tests/integration/test_sstabledump_parity_integration.rs:296` - SSTable dump parity validation
3. `./tests/integration/test_sstabledump_parity_integration.rs:324` - Parity test with real data
4. `./tests/integration/test_sstabledump_parity_integration.rs:351` - Advanced parity validation
5. `./tests/integration/test_sstabledump_parity_integration.rs:391` - Cross-validation tests
6. `./tests/integration/test_sstabledump_parity_integration.rs:495` - Comprehensive parity suite
7. `./tests/integration/test_schema_driven_key_decoding.rs:342` - Schema-driven key decoding (integration test)

### Core Module Tests (High Priority)
8. `./cqlite-core/src/storage/sstable/schema_aware_reader_test.rs:68` - TODO: Fix async SchemaRegistry::new() in tests
9. `./cqlite-core/src/query/select_integration_tests.rs:771` - Performance benchmarks (manual run)
10. `./cqlite-core/src/memory_safety_tests.rs:421` - Memory safety validation

### CLI Tests (Medium Priority)
11. `./cqlite-cli/tests/integration_tests.rs:438` - CLI integration test suite
12. `./cqlite-cli/tests/integration_tests.rs:465` - Extended CLI functionality tests

### Validation Tests (Medium Priority)
13. `./tests/src/issue_36_integration_tests.rs:269` - Performance testing suite
14. `./tests/src/bti_validation.rs:605` - Requires real test data
15. `./tests/src/parser_validation.rs:642` - Requires Docker and test data generation

## Priority Classification

### 🔴 Critical Priority (Convert First)
**SSTable Parity Integration Tests** (6 tests)
- Files: `test_sstabledump_parity_integration.rs`
- Impact: Core functionality validation
- Complexity: Requires Cassandra tools setup
- Estimated effort: 2-3 days

**Schema-Aware Reader Tests** (1 test)
- File: `schema_aware_reader_test.rs`
- Impact: Core reading functionality
- Issue: Async SchemaRegistry initialization
- Estimated effort: 1 day

### 🟡 High Priority
**Core Module Tests** (2 tests)
- Memory safety and query performance tests
- Impact: Core functionality and safety
- Estimated effort: 1-2 days

**Schema-Driven Key Decoding** (1 test)
- Integration test for key decoding
- Impact: Data integrity
- Estimated effort: 1 day

### 🟢 Medium Priority
**CLI Integration Tests** (2 tests)
- CLI functionality validation
- Impact: User interface completeness
- Estimated effort: 1 day

**Validation Tests** (3 tests)
- Performance and parser validation
- Impact: System reliability
- Dependencies: Docker, test data
- Estimated effort: 2 days

## Conversion Strategy

### Phase 1 (Week 1): Critical Tests
1. Fix async SchemaRegistry issue in schema_aware_reader_test.rs
2. Set up Cassandra tools for SSTable parity tests
3. Convert 3-4 highest impact parity tests
4. **Target**: Resolve 6-7 ignored tests

### Phase 2 (Week 2): High Priority Tests
1. Complete remaining parity integration tests
2. Resolve memory safety and query performance tests
3. Convert schema-driven key decoding test
4. **Target**: Resolve 4-5 additional tests

### Phase 3 (Week 3): Remaining Tests
1. Convert CLI integration tests
2. Set up validation test dependencies
3. Complete all remaining ignored tests
4. **Target**: Achieve 0 ignored tests

## Technical Requirements

### Infrastructure Needs
- **Cassandra Tools**: Required for SSTable parity tests
- **Docker**: Needed for parser validation tests
- **Test Data**: Real SSTable files for validation
- **Async Testing**: Fix SchemaRegistry initialization

### Agent Assignments
- **Test Infrastructure Agent**: Setup Cassandra tools, test data generation
- **Core Development Agent**: Fix async issues, implement missing functionality
- **Integration Agent**: Convert integration tests, validate end-to-end flows
- **CLI Agent**: Convert CLI-specific ignored tests

## Success Metrics

### Progress Tracking
- **Week 1**: 15 → 8-9 ignored tests (6-7 converted)
- **Week 2**: 8-9 → 3-4 ignored tests (4-5 converted)
- **Week 3**: 3-4 → 0 ignored tests (all converted)

### Quality Validation
- All converted tests must pass consistently
- No reduction in overall test reliability
- Maintain or improve coverage percentage
- No performance regressions

---

**Status**: ✅ Analysis Complete
**Ready for**: Agent coordination and test conversion
**Estimated Completion**: 2-3 weeks with proper agent coordination