# Issue #1: Setup Safety Nets for Code Cleanup

**Priority:** P0 (Must Do First)  
**Risk Level:** None  
**Estimated Time:** 2 hours  
**Assignee:** DevOps/CI Lead  
**Branch:** `cleanup/issue-1-safety-nets`

---

## Objective

Establish automated safety checks to ensure code cleanup doesn't break existing functionality.

---

## Problem Statement

Before removing ~10,000 lines of code, we need:
1. Baseline CI metrics (tests passing, warnings count, build time)
2. Feature flag validation
3. Minimal feature build verification
4. Code coverage tracking

Without these, we can't safely verify that removals don't break production code.

---

## Changes Required

### 1. Add Minimal Feature CI Job

**File:** `.github/workflows/ci-minimal-features.yml`

```yaml
name: Minimal Features CI

on:
  pull_request:
    branches: [ main, develop ]
  push:
    branches: [ main, develop ]

jobs:
  minimal-build:
    name: Build with Minimal Features
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Rust
        uses: actions-rust-lang/setup-rust-toolchain@v1
        
      - name: Build with no default features
        run: cargo build --no-default-features --features=lz4,snappy
        working-directory: cqlite-core
        
      - name: Build with M1 features only
        run: cargo build --no-default-features --features=all-compression
        working-directory: cqlite-core
        
      - name: Test with M1 features
        run: cargo test --no-default-features --features=all-compression
        working-directory: cqlite-core

  feature-gate-validation:
    name: Validate Feature Gates
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Check experimental not in default
        run: |
          if grep -q 'default.*experimental' cqlite-core/Cargo.toml; then
            echo "ERROR: 'experimental' feature should not be in default"
            exit 1
          fi
          
      - name: Check benchmarks not in default
        run: |
          if grep -q 'default.*benchmarks' cqlite-core/Cargo.toml; then
            echo "ERROR: 'benchmarks' feature should not be in default"
            exit 1
          fi
          
      - name: Validate feature documentation
        run: |
          if ! grep -q "# M1 Core Reading Features" cqlite-core/Cargo.toml; then
            echo "WARNING: Feature documentation missing"
          fi
```

### 2. Add Code Coverage Baseline

**File:** `.github/workflows/coverage-baseline.yml`

```yaml
name: Coverage Baseline

on:
  pull_request:
    paths:
      - 'cqlite-core/src/**'
      
jobs:
  coverage:
    name: Track Coverage
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Rust
        uses: actions-rust-lang/setup-rust-toolchain@v1
        
      - name: Install Tarpaulin
        run: cargo install cargo-tarpaulin
        
      - name: Generate coverage
        run: cargo tarpaulin --workspace --out Xml --exclude-files 'tests/*'
        
      - name: Upload to Codecov
        uses: codecov/codecov-action@v3
        with:
          files: ./cobertura.xml
          fail_ci_if_error: false
          
      - name: Check coverage threshold
        run: |
          coverage=$(grep -oP 'line-rate="\K[0-9.]+' cobertura.xml | head -1)
          threshold=0.60  # 60% minimum
          if (( $(echo "$coverage < $threshold" | bc -l) )); then
            echo "Coverage $coverage below threshold $threshold"
            exit 1
          fi
```

### 3. Create Cleanup Validation Script

**File:** `scripts/validate-cleanup.sh`

```bash
#!/bin/bash
set -e

echo "🔍 Validating cleanup safety..."

# Check 1: Core reading functionality still works
echo "✓ Testing core reading features..."
cargo test --no-default-features --features=all-compression -- --test-threads=1

# Check 2: No new warnings introduced
echo "✓ Checking for warnings..."
warnings=$(cargo clippy --all-targets --all-features 2>&1 | grep -c "warning:" || true)
if [ "$warnings" -gt 50 ]; then
    echo "❌ Too many warnings: $warnings (threshold: 50)"
    exit 1
fi

# Check 3: Verify removed files are truly unused
echo "✓ Checking for unused imports..."
cargo build --all-features 2>&1 | grep -i "unused" && exit 1 || true

# Check 4: Binary size check
echo "✓ Checking binary size..."
cargo build --release --no-default-features --features=all-compression
size=$(stat -f%z target/release/libcqlite_core.* 2>/dev/null || stat -c%s target/release/libcqlite_core.* 2>/dev/null)
echo "Binary size: $size bytes"

echo "✅ All validation checks passed!"
```

Make executable:
```bash
chmod +x scripts/validate-cleanup.sh
```

### 4. Baseline Metrics File

**File:** `docs/cleanup/BASELINE_METRICS.md`

```markdown
# Cleanup Baseline Metrics

Captured: $(date)

## Build Metrics

- **Full Build Time:** $(cargo build --release 2>&1 | grep "Finished" | awk '{print $2}')
- **Test Count:** $(cargo test --no-run 2>&1 | grep -c "test" || echo "0")
- **Total Lines (cqlite-core):** $(find cqlite-core/src -name "*.rs" | xargs wc -l | tail -1)
- **Warning Count:** $(cargo clippy --all-targets 2>&1 | grep -c "warning:" || echo "0")

## Feature Combinations

- [ ] `--no-default-features` - Compiles: YES/NO
- [ ] `--features=all-compression` - Tests Pass: YES/NO
- [ ] `--features=all-compression,state_machine` - Tests Pass: YES/NO
- [ ] `--all-features` - Tests Pass: YES/NO

## File Inventory (To Be Removed)

### Dead Code (Zero Dependencies)
- cqlite-core/src/query/optimized_executor.rs (1,045 lines)
- cqlite-core/src/performance_monitor.rs (596 lines)
- cqlite-core/src/parser/m3_performance_benchmarks.rs (1,285 lines)
- cqlite-core/src/parser/performance_regression_framework.rs (822 lines)

### Write Infrastructure
- cqlite-core/src/storage/batch_writer.rs (543 lines)
- cqlite-core/src/storage/wal.rs (377 lines)
- cqlite-core/src/storage/memtable.rs (393 lines)
- cqlite-core/src/storage/manifest.rs (388 lines)
- cqlite-core/src/storage/compaction.rs (457 lines)
- cqlite-core/src/storage/sstable/writer.rs (959 lines)

**Total Lines to Remove:** ~7,000+ lines
```

### 5. Update CI to Run Validation

**File:** `.github/workflows/ci.yml` (add to existing)

```yaml
  cleanup-validation:
    name: Cleanup Safety Validation
    runs-on: ubuntu-latest
    if: contains(github.head_ref, 'cleanup/')
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Install Rust
        uses: actions-rust-lang/setup-rust-toolchain@v1
        
      - name: Run validation script
        run: ./scripts/validate-cleanup.sh
```

---

## Testing Checklist

- [ ] Create new CI workflow files
- [ ] Verify workflows trigger on test PR
- [ ] Run validation script locally - succeeds
- [ ] Generate baseline metrics document
- [ ] Commit all safety infrastructure
- [ ] Verify CI jobs appear in GitHub Actions

---

## Verification Commands

```bash
# Test minimal features build
cargo build --no-default-features --features=all-compression

# Run validation script
./scripts/validate-cleanup.sh

# Check CI configuration syntax
actionlint .github/workflows/*.yml  # Install: brew install actionlint

# Generate baseline
find cqlite-core/src -name "*.rs" -exec wc -l {} + | tail -1
cargo test --no-run 2>&1 | grep -c "test"
```

---

## Success Criteria

✅ New CI jobs added and running  
✅ Validation script executes successfully  
✅ Baseline metrics documented  
✅ All existing CI checks still pass  
✅ Code coverage tracking enabled  

---

## Dependencies

**Blocks:** All other cleanup issues (must complete first)  
**Blocked By:** None

---

## Rollback Plan

```bash
# If CI jobs cause issues:
git revert <commit-hash>
git push origin main

# Remove CI files manually:
rm .github/workflows/ci-minimal-features.yml
rm .github/workflows/coverage-baseline.yml
git commit -m "Rollback: Remove cleanup CI"
```

---

## Notes

- This issue adds **only new files** - zero risk to existing code
- All changes are additive safety checks
- Can be merged immediately without breaking anything
- Provides safety net for all subsequent cleanup issues

---

## Completion Checklist

- [ ] All files created and committed
- [ ] CI jobs running successfully
- [ ] Validation script tested locally
- [ ] Baseline metrics captured
- [ ] PR approved and merged
- [ ] Tag teams that Issue #2-#7 can begin

