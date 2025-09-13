# SSTable Companion File Parity Test Architecture

## Issue #31 Implementation Design

This document outlines the comprehensive test architecture for validating zero-diff parity between CQLite's SSTable companion file parsing and Cassandra's sstabledump utility using real Cassandra 5 data.

## Architecture Overview

The test system is designed with a component-first architecture that provides deterministic, minimal test execution suitable for CI environments while ensuring comprehensive validation coverage.

### Core Components

```
cqlite-core/tests/
├── sstabledump_parity_index.rs      # Index.db parity validation
├── sstabledump_parity_summary.rs    # Summary.db parity validation  
├── sstabledump_parity_statistics.rs # Statistics.db parity validation
└── sstabledump_parity_orchestrator.rs # Test coordination and execution
```

## Design Principles

### 1. Deterministic Table Selection

**Requirement**: Target 2-3 representative tables from `metadata.yml`

**Implementation**:
- `test_basic.simple_table` (1000 rows) - Standard table structure
- `test_timeseries.sensor_data` (2000 rows) - Time series patterns
- `test_wide_rows.wide_partition_table` (100 rows) - Wide partitions for promoted index testing

**Rationale**: Fixed selection ensures repeatable results across CI runs while covering diverse SSTable patterns.

### 2. Companion File Derivation

**Requirement**: Derive companions from Data.db files without hardcoding names

**Implementation**:
```rust
fn derive_companion_file_path(data_db_path: &Path, component: &str) -> Result<PathBuf> {
    let file_name = data_db_path.file_name()?.to_str()?;
    let companion_name = file_name.replace("-Data.db", &format!("-{}.db", component));
    Ok(data_db_path.parent()?.join(companion_name))
}
```

**Examples**:
- `nb-1-big-Data.db` → `nb-1-big-Index.db`
- `nb-1-big-Data.db` → `nb-1-big-Summary.db`  
- `nb-1-big-Data.db` → `nb-1-big-Statistics.db`

### 3. Zero-Diff Parity Validation

**Requirement**: Assert exact matching, not just "no crash"

**Implementation Strategy**:
- Generate sstabledump JSON output for each companion file
- Parse equivalent structures with CQLite
- Perform field-by-field comparison with tolerance only where specified
- Save all inputs, outputs, and diffs as validation artifacts

### 4. Fast-Fail Error Handling

**Requirement**: Eliminate fallback behaviors, fail clearly when datasets missing

**Implementation**:
```rust
match resolve_table_to_sstable_path(keyspace, table) {
    Ok(path) => proceed_with_test(path),
    Err(e) => panic!("Dataset missing: {}. Use canonical datasets only.", e),
}
```

## Component Architecture

### Index.db Parity Tests (`sstabledump_parity_index.rs`)

**Responsibilities**:
- Validate key digest accuracy
- Verify data offset correctness
- Test promoted index path for wide partition tables
- Assert partition key lookups return correct rows

**Key Features**:
- Deterministic partition sampling (max 10 partitions for CI performance)
- Wide partition detection and promoted index validation
- Offset verification against actual Data.db positions

### Summary.db Parity Tests (`sstabledump_parity_summary.rs`)

**Responsibilities**:
- Validate entry ordering (monotonic token sequence)
- Verify token coverage and sampling consistency
- Test summary entry structure integrity

**Key Features**:
- Token range validation with configurable sampling (max 20 ranges)
- Monotonic ordering verification
- Coverage calculation and comparison
- Fixed-seed sampling for deterministic results

### Statistics.db Parity Tests (`sstabledump_parity_statistics.rs`)

**Responsibilities**:
- Validate checksum/CRC integrity
- Verify metadata invariants (timestamps > 0, live_rows ≤ total_rows)
- Compare row counts against metadata.yml with tolerance
- Test compression ratio and histogram data

**Key Features**:
- Invariant validation for data consistency
- Metadata cross-reference with 5% tolerance
- Checksum format and integrity verification
- Timestamp range validation

### Orchestrator (`sstabledump_parity_orchestrator.rs`)

**Responsibilities**:
- Coordinate execution across all components
- Manage artifact generation and cleanup
- Provide comprehensive result reporting
- Enforce CI-friendly timeouts

**Key Features**:
- Pre-flight dataset availability validation
- Parallel execution support
- Comprehensive result aggregation
- Performance monitoring and reporting

## Artifact Management

### Directory Structure
```
validation_artifacts/sstabledump/<keyspace.table>/
├── sstabledump_index.json       # sstabledump Index.db output
├── cqlite_index.json           # CQLite Index.db output
├── index_diff.txt              # Index comparison result
├── sstabledump_summary.json    # sstabledump Summary.db output
├── cqlite_summary.json         # CQLite Summary.db output  
├── summary_validation.txt      # Summary comparison result
├── sstabledump_statistics.json # sstabledump Statistics.db output
├── cqlite_statistics.json      # CQLite Statistics.db output
└── statistics_validation.txt   # Statistics comparison result
```

### Artifact Lifecycle
- Generated fresh for each test run
- Retained for debugging failed tests
- Cleaned up automatically to prevent disk bloat
- Limited to 10 artifacts per table by default

## Performance and CI Considerations

### Execution Bounds
- Maximum test execution time: 5 minutes (300 seconds)
- Maximum partitions sampled per table: 10
- Maximum token ranges sampled: 20
- Deterministic sampling with fixed seeds

### CI Integration
- Tests placed in `cqlite-core/tests/` for automatic pickup
- No workflow changes required
- Relies on unified `datasets-v2` full dataset cache
- Fast-fail behavior prevents hanging builds

## Error Handling Strategy

### Classification of Failures

**Dataset Missing (Fast-Fail)**:
```rust
Err(ParityError::DatasetMissing(msg)) => {
    panic!("Dataset missing: {}. Ensure canonical datasets are available.", msg);
}
```

**Parity Validation Failed**:
```rust
Err(ParityError::ParityFailed(details)) => {
    // Save artifacts and report specific differences
    save_validation_artifacts(...)?;
    panic!("Zero-diff validation failed: {}", details);
}
```

**Infrastructure Failures**:
```rust
Err(ParityError::SstabledumpFailed(msg)) => {
    panic!("sstabledump command failed: {}", msg);
}
```

## Testing Strategy

### Unit Tests
- Companion file derivation logic
- JSON parsing and comparison utilities
- Invariant validation functions
- Configuration determinism verification

### Integration Tests  
- End-to-end parity validation per component
- Cross-component orchestration
- Dataset availability checking
- Artifact generation and cleanup

### Acceptance Criteria Validation
- ✓ Canonical datasets only (no synthetic/mocks)
- ✓ Deterministic 2-3 tables including wide partition
- ✓ Companions derived from Data.db prefixes  
- ✓ Zero-diff artifacts saved under `validation_artifacts/`
- ✓ Component-first naming in `cqlite-core/tests/`
- ✓ Green CI with dataset cache restore

## Future Extensibility

### Adding New Components
```rust
// New component test module
mod sstabledump_parity_filter;

// Register in orchestrator
fn run_filter_parity_tests(config: &ParityTestSuite, results: &mut ParityTestResults) -> Result<(), String> {
    // Implementation
}
```

### Scaling Test Coverage
- Add new table types via configuration
- Extend sampling strategies
- Support additional SSTable format versions
- Integrate with property-based testing

## Dependencies

### External Tools
- `sstabledump` (Cassandra utility)
- Canonical datasets under `test-data/datasets/`

### Internal Dependencies
- `cqlite_core::testing::{list_tables, resolve_table_to_sstable_path, load_metadata}`
- `serde_json` for JSON processing
- `thiserror` for error handling
- `hex` for checksum validation

## Monitoring and Observability

### Metrics Collected
- Test execution time per component
- Number of tables/partitions tested
- Artifacts generated count
- Error rates by failure type

### Reporting Format
```
SSTable Parity Test Suite Results:
✓ Tables tested: 3
✓ Components tested: 3  
✓ Total tests: 9
✓ Passed: 9
✓ Failed: 0
✓ Execution time: 45000ms
✓ Artifacts generated: 27
```

This architecture ensures robust, deterministic testing of SSTable companion file parity while maintaining CI performance and providing comprehensive debugging capabilities through detailed artifact generation.