# Remaining Import/Dependency Issues Analysis

## Executive Summary

After running `cargo check`, 6 critical compilation errors remain that need to be addressed. These errors fall into two main categories:

1. **Ungated M3 Performance Module Imports** (4 errors)
2. **Tombstone Merger Feature Gating Issues** (2 errors)

## Detailed Error Analysis

### 1. Ungated M3 Performance Module Imports

#### Error 1: benchmarks module
**File:** `cqlite-core/src/parser/mod.rs:111`
**Error:** `unresolved import 'benchmarks'`
**Issue:** Module is gated with `#[cfg(feature = "benchmarks")]` but re-export is not gated

#### Error 2: m3_performance_benchmarks module  
**File:** `cqlite-core/src/parser/mod.rs:121`
**Error:** `unresolved import 'm3_performance_benchmarks'`
**Issue:** Module declaration (line 60) is NOT gated but the actual module file has `#![cfg(feature = "benchmarks")]`

#### Error 3: performance_regression_framework module
**File:** `cqlite-core/src/parser/mod.rs:123`  
**Error:** `unresolved import 'performance_regression_framework'`
**Issue:** Module declaration (line 62) is NOT gated but the actual module file has `#![cfg(feature = "benchmarks")]`

#### Error 4: performance module in validation
**File:** `cqlite-core/src/validation/mod.rs:474`
**Error:** `unresolved import 'performance'`
**Issue:** Re-export line is NOT gated with `#[cfg(feature = "benchmarks")]`

### 2. Tombstone Merger Feature Gating Issues

#### Error 5: tombstone_merger import in reader.rs
**File:** `cqlite-core/src/storage/sstable/reader.rs:35`
**Error:** `unresolved import 'super::tombstone_merger'`
**Issue:** Import line is gated with `#[cfg(feature = "tombstones")]` but the usage spans multiple lines and conditions

#### Error 6: tombstone_merger import in mod.rs
**File:** `cqlite-core/src/storage/sstable/mod.rs:45`
**Error:** `unresolved import 'self::tombstone_merger'`
**Issue:** Import line is gated with `#[cfg(feature = "tombstones")]` but the usage is not properly conditional

## Files That Need Fixing

### High Priority Fixes Required

1. **`/Users/patrick/local_projects/cqlite/cqlite-core/src/parser/mod.rs`**
   - Lines 60, 62: Add `#[cfg(feature = "benchmarks")]` to m3_performance_benchmarks and performance_regression_framework module declarations
   - Lines 121, 123: Add `#[cfg(feature = "benchmarks")]` to re-export statements

2. **`/Users/patrick/local_projects/cqlite/cqlite-core/src/validation/mod.rs`**
   - Line 474: Add `#[cfg(feature = "benchmarks")]` to performance module re-export

3. **`/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/reader.rs`**
   - Line 35: Fix tombstone_merger import conditional compilation
   - Review all TombstoneMerger and GenerationValue usage for proper feature gating

4. **`/Users/patrick/local_projects/cqlite/cqlite-core/src/storage/sstable/mod.rs`**
   - Line 45: Fix tombstone_merger import conditional compilation
   - Review all EntryMetadata, GenerationValue, TombstoneMerger usage for proper feature gating

## Feature Gate Analysis

### Current Feature Gates Found
- `feature = "benchmarks"` - Performance benchmarking code
- `feature = "tombstones"` - Tombstone merger functionality  
- `feature = "legacy-heuristics"` - Legacy format support
- `feature = "snappy"` - Snappy compression
- `feature = "deflate"` - Deflate compression
- `feature = "zstd"` - ZSTD compression
- `feature = "lz4"` - LZ4 compression
- `feature = "state_machine"` - State machine tests
- `feature = "pest"` - PEST parser backend

### Inconsistent Gating Patterns

**Pattern 1: Module declared but not gated**
```rust
// WRONG - module exists but not gated
pub mod m3_performance_benchmarks;  // line 60

// File has gating at top
#![cfg(feature = "benchmarks")]     // in the .rs file
```

**Pattern 2: Re-export not gated**
```rust
// Module gated correctly
#[cfg(feature = "benchmarks")]
pub mod benchmarks;

// Re-export not gated - WRONG
pub use benchmarks::*;  // line 111
```

**Pattern 3: Correct gating**
```rust
// Module gated
#[cfg(feature = "benchmarks")]
pub mod performance;

// Re-export gated - CORRECT
#[cfg(feature = "benchmarks")]
pub use performance::{PerformanceMetrics, PerformanceTestCase};
```

## Specific Code Changes Needed

### File: `cqlite-core/src/parser/mod.rs`

**Current lines 59-62:**
```rust
// M3 Performance Optimization Modules
pub mod m3_performance_benchmarks;
pub mod optimized_complex_types;
pub mod performance_regression_framework;
```

**Should be:**
```rust
// M3 Performance Optimization Modules
#[cfg(feature = "benchmarks")]
pub mod m3_performance_benchmarks;
pub mod optimized_complex_types;
#[cfg(feature = "benchmarks")]
pub mod performance_regression_framework;
```

**Current lines 121-123:**
```rust
// Re-export M3 performance modules
pub use m3_performance_benchmarks::{M3PerformanceBenchmarks, PerformanceTargets};
pub use optimized_complex_types::OptimizedComplexTypeParser;
pub use performance_regression_framework::{PerformanceRegressionFramework, RegressionThresholds};
```

**Should be:**
```rust
// Re-export M3 performance modules
#[cfg(feature = "benchmarks")]
pub use m3_performance_benchmarks::{M3PerformanceBenchmarks, PerformanceTargets};
pub use optimized_complex_types::OptimizedComplexTypeParser;
#[cfg(feature = "benchmarks")]
pub use performance_regression_framework::{PerformanceRegressionFramework, RegressionThresholds};
```

### File: `cqlite-core/src/validation/mod.rs`

**Current line 474:**
```rust
pub use performance::{PerformanceMetrics, PerformanceTestCase};
```

**Should be:**
```rust
#[cfg(feature = "benchmarks")]
pub use performance::{PerformanceMetrics, PerformanceTestCase};
```

## Testing Strategy

After implementing fixes:

1. **Basic Compilation Test:**
   ```bash
   cargo check
   ```

2. **Feature-specific Tests:**
   ```bash
   cargo check --features benchmarks
   cargo check --features tombstones
   cargo check --no-default-features
   ```

3. **Full Build Test:**
   ```bash
   cargo build
   cargo test --features benchmarks,tombstones
   ```

## Risk Assessment

**Low Risk:** The fixes are straightforward feature gating additions that should not impact existing functionality.

**Medium Risk:** Tombstone merger fixes may require more careful review to ensure all conditional compilation is correct.

**High Risk:** None identified - these are purely compilation fixes.

## Next Steps

1. ✅ **COMPLETED:** Analysis and identification of all remaining issues
2. 🔄 **IN PROGRESS:** Implement fixes for ungated M3 performance modules
3. ⏳ **PENDING:** Implement fixes for tombstone merger imports  
4. ⏳ **PENDING:** Verify conditional compilation patterns
5. ⏳ **PENDING:** Run comprehensive compilation tests
6. ⏳ **PENDING:** Document any feature combinations that are incompatible