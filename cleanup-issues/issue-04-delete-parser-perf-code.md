# Issue #4: Delete Parser Performance Code (Dead Code)

**Priority:** P0  
**Risk Level:** Zero (Never Called)  
**Estimated Time:** 1 hour  
**Assignee:** Team A  
**Branch:** `cleanup/issue-4-delete-parser-perf-code`  
**Can Parallelize:** ✅ Yes (with Issues #2, #3, #5)

---

## Objective

Remove 2,107 lines of parser performance benchmarking code that is never executed.

---

## Problem Statement

Two large files in the parser module implement M3 performance benchmarks and regression frameworks:
- `m3_performance_benchmarks.rs` (1,285 lines)
- `performance_regression_framework.rs` (822 lines)

**Evidence they're dead:**
```bash
$ grep -r "M3PerformanceBenchmarks::new" cqlite-core/src/
# NO MATCHES

$ grep -r "PerformanceRegressionFramework" cqlite-core/src/
# NO MATCHES
```

These are re-exported from `parser/mod.rs` but never instantiated anywhere.

**Additional issue:** Why are "M3" (output format) benchmarks in the parser module?

---

## Files to Delete

```
cqlite-core/src/parser/m3_performance_benchmarks.rs           (1,285 lines)
cqlite-core/src/parser/performance_regression_framework.rs    (822 lines)
```

**Total:** 2,107 lines

---

## Files to Modify

### 1. `cqlite-core/src/parser/mod.rs`

**Remove:**
```rust
pub mod m3_performance_benchmarks;
pub mod performance_regression_framework;

// And any re-exports:
pub use m3_performance_benchmarks::{M3PerformanceBenchmarks, PerformanceTargets};
pub use performance_regression_framework::*;
```

---

## Step-by-Step Instructions

### Step 1: Create Branch

```bash
git checkout -b cleanup/issue-4-delete-parser-perf-code
```

### Step 2: Verify Files Are Unused

```bash
# Check for usage
grep -r "M3PerformanceBenchmarks" cqlite-core/src/ | grep -v "\.rs:"
grep -r "PerformanceRegressionFramework" cqlite-core/src/
grep -r "use.*m3_performance" cqlite-core/src/
grep -r "use.*performance_regression" cqlite-core/src/

# Expected: No matches in production code
```

### Step 3: Delete Files

```bash
git rm cqlite-core/src/parser/m3_performance_benchmarks.rs
git rm cqlite-core/src/parser/performance_regression_framework.rs
```

### Step 4: Remove Module Declarations

Edit `cqlite-core/src/parser/mod.rs`:

Find and remove:
```rust
pub mod m3_performance_benchmarks;
pub mod performance_regression_framework;

// Also remove re-exports (search for):
pub use m3_performance_benchmarks::*;
pub use performance_regression_framework::*;
```

### Step 5: Verify Compilation

```bash
cd cqlite-core
cargo build --all-features
cargo test --all-features
```

### Step 6: Run Validation

```bash
./scripts/validate-cleanup.sh
```

---

## Testing Checklist

- [ ] Verify files unused: `grep -r "m3_performance\|performance_regression" cqlite-core/src/`
- [ ] Delete both files
- [ ] Update parser/mod.rs (remove declarations and exports)
- [ ] `cargo build --all-features` - success
- [ ] `cargo test --all-features` - pass
- [ ] `./scripts/validate-cleanup.sh` - success
- [ ] No new warnings
- [ ] Commit: "Remove parser performance code (M3/M6 scope, never used)"

---

## Verification Commands

```bash
# Search for any dependencies
rg "M3Performance|PerformanceRegression" cqlite-core/src/ --type rust

# Verify parser module still compiles
cargo build --package cqlite-core --lib

# Run parser tests specifically
cargo test --package cqlite-core parser::

# Check for broken re-exports
cargo doc --no-deps 2>&1 | grep -i error
```

---

## Expected Impact

- **Lines Removed:** 2,107
- **Modules Removed:** 2 from parser
- **Breaking Changes:** None (types not used externally)
- **Build Time:** Slightly faster

---

## Success Criteria

✅ Both files deleted  
✅ parser/mod.rs updated  
✅ All builds succeed  
✅ Parser tests still pass  
✅ No broken documentation links  
✅ CI green  

---

## Dependencies

**Requires:** Issue #1 complete  
**Blocks:** None  
**Can Parallelize With:** Issues #2, #3, #5, #6, #7

---

## Potential Complications

**Q: What if these files are referenced in benchmarks/?**

A: Check with:
```bash
grep -r "m3_performance\|performance_regression" cqlite-core/src/benchmarks/
```

If found, those are dead code too (benchmarks are never run). Safe to delete.

**Q: What if Cargo.toml has a benchmark harness entry?**

A: Check:
```bash
grep -A2 "\[\[bench\]\]" cqlite-core/Cargo.toml
```

If these benchmarks are listed, remove the `[[bench]]` entries as well.

---

## Rollback Plan

```bash
git revert <commit-hash>
# Or:
git checkout main -- cqlite-core/src/parser/m3_performance_benchmarks.rs
git checkout main -- cqlite-core/src/parser/performance_regression_framework.rs
git checkout main -- cqlite-core/src/parser/mod.rs
git commit -m "Rollback: Restore parser perf code"
```

---

## CI Checks

- ✅ Build (all features)
- ✅ Test (parser module specifically)
- ✅ Doc generation
- ✅ Clippy (no new warnings)

---

## Notes

- These files are likely behind `#[cfg(feature = "benchmarks")]` but still compiled
- Removing them will speed up compilation
- No production code depends on them
- Good candidate for parallel work with Issues #2 and #3

---

## Completion Checklist

- [ ] Branch created
- [ ] Both files verified as unused
- [ ] Files deleted
- [ ] parser/mod.rs updated
- [ ] Builds successful
- [ ] Parser tests pass
- [ ] Validation passes
- [ ] PR created
- [ ] CI green
- [ ] Merged

