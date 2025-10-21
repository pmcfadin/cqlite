# Issue #3: Delete PerformanceMonitor (Dead Code)

**Priority:** P0  
**Risk Level:** Zero (Only Used in Own Tests)  
**Estimated Time:** 1 hour  
**Assignee:** Team A  
**Branch:** `cleanup/issue-3-delete-performance-monitor`  
**Can Parallelize:** ✅ Yes (with Issues #2, #4, #5)

---

## Objective

Remove 596 lines of performance monitoring code that is never used in production.

---

## Problem Statement

`cqlite-core/src/performance_monitor.rs` implements a performance baseline tracking and regression detection system. It's only used in its own unit tests.

**Evidence:**
```bash
$ grep -r "PerformanceMonitor::new" cqlite-core/src/ | grep -v performance_monitor.rs
# NO MATCHES - only called from its own tests
```

This is M6 territory ("Perf & Size Validation"), not M1/M2.

---

## Files to Delete

```
cqlite-core/src/performance_monitor.rs  (596 lines)
```

---

## Files to Modify

### 1. `cqlite-core/src/lib.rs`

**Remove:**
```rust
pub mod performance_monitor;
```

### 2. Search for Any Imports

Need to verify no other code imports this module.

---

## Step-by-Step Instructions

### Step 1: Create Branch

```bash
git checkout -b cleanup/issue-3-delete-performance-monitor
```

### Step 2: Verify Usage

```bash
# Check for any production usage
grep -r "PerformanceMonitor" cqlite-core/src/ | grep -v "performance_monitor.rs"
grep -r "use.*performance_monitor" cqlite-core/src/

# Expected: No matches
```

### Step 3: Delete File

```bash
git rm cqlite-core/src/performance_monitor.rs
```

### Step 4: Remove Module Declaration

Edit `cqlite-core/src/lib.rs`:

```rust
// DELETE this line:
pub mod performance_monitor;
```

### Step 5: Verify Compilation

```bash
cd cqlite-core
cargo build --all-features
cargo test --all-features
```

**Expected:** Clean compilation, all tests pass.

### Step 6: Run Validation

```bash
./scripts/validate-cleanup.sh
```

---

## Testing Checklist

- [ ] Search for usage: `grep -r "PerformanceMonitor" cqlite-core/src/`
- [ ] Delete file: `git rm cqlite-core/src/performance_monitor.rs`
- [ ] Remove from lib.rs
- [ ] `cargo build --all-features` - success
- [ ] `cargo test --all-features` - all pass
- [ ] `./scripts/validate-cleanup.sh` - success
- [ ] No new warnings
- [ ] Commit: "Remove PerformanceMonitor (M6 scope, unused in M1/M2)"

---

## Verification Commands

```bash
# Verify no dependencies
rg "performance_monitor" cqlite-core/src/ --type rust

# Clean build
cargo clean
cargo build --all-features
cargo test --all-features

# Check feature flags still work
cargo build --no-default-features --features=all-compression
```

---

## Expected Impact

- **Lines Removed:** 596
- **Features Removed:** Performance baseline tracking, regression detection
- **Breaking Changes:** None (not in public API)
- **Test Count:** May decrease slightly (only if file had unit tests)

---

## Success Criteria

✅ File deleted  
✅ Module declaration removed  
✅ All builds succeed  
✅ All tests pass  
✅ No compiler warnings  
✅ CI green  

---

## Dependencies

**Requires:** Issue #1 complete  
**Blocks:** None  
**Can Parallelize With:** Issues #2, #4, #5, #6, #7

---

## Rollback Plan

```bash
git revert <commit-hash>
# Or:
git checkout main -- cqlite-core/src/performance_monitor.rs
git checkout main -- cqlite-core/src/lib.rs
git commit -m "Rollback: Restore PerformanceMonitor"
```

---

## CI Checks

- ✅ Build (all features)
- ✅ Test (all features)
- ✅ Build (minimal features)
- ✅ Clippy (no new warnings)

---

## Notes

- This file is feature-gated behind `#[cfg(feature = "benchmarks")]` at the top
- Never imported by production code
- Safe to delete with zero impact

---

## Completion Checklist

- [ ] Branch created
- [ ] Usage verified as none
- [ ] File deleted
- [ ] lib.rs updated
- [ ] Builds successful
- [ ] Tests pass
- [ ] Validation passes
- [ ] PR created
- [ ] CI green
- [ ] Merged

