# Mock Usage Policy - Issue #80 Implementation

## Overview

This document defines the mock usage policy implemented to satisfy Issue #80 requirements: **Remove mocks from M1-critical integration paths (file-backed only)**.

## Policy Summary

✅ **ALLOWED**: Mock usage in unit tests with `unit-tests-only` feature flag
🚫 **PROHIBITED**: Mock usage in M1-critical integration paths (CI default lane)

## Implementation Details

### Feature Flag Control

**Feature Flag**: `unit-tests-only`
- **Purpose**: Gates mock/synthetic data usage to unit tests only
- **Default**: Disabled (mocks not available by default)
- **CI Behavior**: Never enabled in CI integration paths

### File Locations

#### Cargo.toml Configuration
```toml
# tests/Cargo.toml — the flag lives HERE, where its 25 cfg sites are
[features]
unit-tests-only = []  # Enables mock/synthetic data usage for unit tests
```

> The matching `cqlite-core` declaration (and this feature's forward to it) were
> deleted in #1698: core had **zero** `cfg(feature = "unit-tests-only")` sites, so
> enabling it there changed nothing.

#### Protected Mock Functions
- `tests/src/real_sstable_test_fixtures.rs:783` - `write_mock_sstable()`
- `tests/src/cli_integration_tests.rs:139` - Mock SSTable creation

### Provenance Gate Integration

**Script**: `scripts/ci/ensure_real_dataset.sh`
- Enforces real-only datasets in CI
- Detects and blocks `unit-tests-only` feature usage
- Enhanced error messages with remediation steps

### CI Compliance

**M1 CI Pipeline** (`.github/workflows/m1-ci.yml`)
- ✅ Downloads real Cassandra 5 datasets
- ✅ SHA256 verification of dataset integrity
- ✅ Provenance gate active (lines 372-388)
- ✅ No mock feature flags enabled

## Usage Examples

### ✅ Correct: Unit Testing with Mocks
```bash
# Enable mocks for unit testing
cargo test --features unit-tests-only

# Specific test with mock fixtures
cargo test test_mock_sstable_generation --features unit-tests-only
```

### 🚫 Blocked: Integration Testing with Mocks
```bash
# This will fail in CI - no mock features enabled
cargo test --test integration_tests

# CI automatically rejects unit-tests-only feature
FEATURES="unit-tests-only" scripts/ci/ensure_real_dataset.sh "test args"
```

### ✅ Required: Integration Testing with Real Data
```bash
# Download real datasets first
gh release download datasets-v2 --pattern "cassandra5-small-refs-only-v2.tar.gz"
tar -xzf cassandra5-small-refs-only-v2.tar.gz

# Run integration tests with real data
cargo test --test sstabledump_parity_integration
cargo test --test cassandra_compatibility
```

## Error Messages

When mocks are disabled, clear error messages guide users:

```
Mock SSTable generation disabled: use real datasets only (Issue #80).
Enable 'unit-tests-only' feature for unit testing with mocks.
```

```
❌ PARITY TEST FAILURE - No real dataset indicators found.

🔍 Issue #80 requires all M1-critical integration paths to use REAL datasets only.

📋 Required actions:
  1. Ensure test-data/datasets/ contains real Cassandra 5 SSTables
  2. Download datasets: gh release download datasets-v2 --pattern 'cassandra5-small-refs-only-v2.tar.gz'
  3. Verify SHA256: 1cfd054d7236132417fc93e91d17f660bbb96f6c5562f19ddc5c12e50bfbf2df
  4. Extract to project root: tar -xzf cassandra5-small-refs-only-v2.tar.gz

💡 For unit tests with mocks, enable the 'unit-tests-only' feature flag.
🚫 Mock/synthetic datasets are prohibited in CI integration paths per Issue #80.
```

## Compliance Verification

### Issue #80 Acceptance Criteria Status

- [x] **All integration tests open real files** - ✅ IMPLEMENTED
  - M1 CI downloads real Data.db/Index.db/Summary.db/Statistics.db files
  - Mock functions gated behind `unit-tests-only` feature

- [x] **CI default lane runs with mocks disabled and passes** - ✅ IMPLEMENTED
  - No mock features enabled in `.github/workflows/m1-ci.yml`
  - Provenance gate actively blocks mock usage

- [x] **Tests fail fast if datasets missing** - ✅ IMPLEMENTED
  - Enhanced error messages with clear remediation steps
  - Provenance gate provides actionable instructions

- [x] **Provenance gate (#79) enabled in CI** - ✅ IMPLEMENTED
  - Active in M1 CI pipeline (lines 372-388)
  - Blocks synthetic/mock patterns including `unit-tests-only`

## Development Workflow

### For Unit Test Development
1. Enable `unit-tests-only` feature locally
2. Use mock fixtures for fast iteration
3. Ensure tests also pass with real data before submission

### For Integration Test Development
1. Always use real datasets from `test-data/datasets/`
2. Download datasets if missing: `gh release download datasets-v2`
3. Verify tests work with actual Cassandra 5 SSTables

### For CI Submission
1. Never enable `unit-tests-only` in CI configurations
2. Ensure real datasets are available
3. Provenance gate will enforce compliance automatically

## Summary

Issue #80 is **FULLY IMPLEMENTED** with:
- ✅ Mock usage gated behind `unit-tests-only` feature (disabled by default)
- ✅ CI integration paths use real datasets only
- ✅ Provenance gate actively enforces real-data-only policy
- ✅ Clear error messages guide proper usage
- ✅ M1 CI pipeline compliant with all requirements

This implementation satisfies all acceptance criteria while maintaining flexibility for unit test development.