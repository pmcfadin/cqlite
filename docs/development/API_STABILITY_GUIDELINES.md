# API Stability Guidelines

## Overview
This document establishes guidelines for maintaining API stability and preventing the compilation errors experienced in Phase 1 and 2 of the CQLite project.

## Breaking Change Prevention

### 1. Deprecation Before Removal
When changing public APIs, follow this pattern:

```rust
// Step 1: Mark as deprecated in one release
#[deprecated(since = "0.1.1", note = "Use `new_method()` instead")]
pub fn old_method(&self) -> Result<T> {
    self.new_method()
}

pub fn new_method(&self) -> Result<T> {
    // New implementation
}
```

### 2. Compatibility Wrappers
Instead of making fields private immediately, add getter methods:

```rust
pub struct TestResult {
    // Keep public for backward compatibility
    pub passed: bool,
    pub errors: Vec<String>,
    
    // Private versions for internal use
    _passed: bool,
    _errors: Vec<String>,
}

impl TestResult {
    // Provide getters for controlled access
    pub fn passed(&self) -> bool {
        self._passed
    }
    
    pub fn errors(&self) -> &[String] {
        &self._errors
    }
}
```

### 3. Feature Flags for Breaking Changes
Use feature flags to introduce breaking changes gradually:

```toml
[features]
default = ["stable-api"]
stable-api = []
breaking-changes-v2 = []
```

## API Design Principles

### 1. Builder Pattern for Complex APIs
```rust
pub struct QueryBuilder {
    // Internal fields
}

impl QueryBuilder {
    pub fn new() -> Self { /* */ }
    pub fn with_table(mut self, table: &str) -> Self { /* */ }
    pub fn with_limit(mut self, limit: usize) -> Self { /* */ }
    pub fn build(self) -> Result<Query> { /* */ }
}
```

### 2. Extensible Enums
Use non-exhaustive enums for forward compatibility:

```rust
#[non_exhaustive]
pub enum CompressionType {
    Lz4,
    Snappy,
    Deflate,
    Zstd,
}
```

### 3. Version-Aware APIs
```rust
#[derive(Debug, Clone)]
pub enum ApiVersion {
    V1,
    V2,
    #[doc(hidden)]
    __NonExhaustive,
}

pub trait VersionedApi {
    fn supported_versions() -> &'static [ApiVersion];
}
```

## Testing API Stability

### 1. Compilation Tests
Use trybuild for API compatibility tests:

```rust
#[test]
fn api_compatibility() {
    let t = trybuild::TestCases::new();
    t.pass("tests/api-compat/*.rs");
    t.compile_fail("tests/api-breaks/*.rs");
}
```

### 2. Semantic Versioning
- Patch (0.1.X): Bug fixes only
- Minor (0.X.0): Backward compatible additions
- Major (X.0.0): Breaking changes allowed

## CI Integration

The `ci_zero_tolerance` feature enables strict checking:

```bash
# In CI, enable zero tolerance mode
cargo test --features ci_zero_tolerance
cargo clippy --features ci_zero_tolerance -- -D warnings
```

## Migration Strategies

### 1. Two-Phase Migration
1. **Phase 1**: Add new API alongside old
2. **Phase 2**: Remove old API after deprecation period

### 2. Adapter Pattern
```rust
pub struct LegacyAdapter {
    inner: NewImplementation,
}

impl LegacyAdapter {
    pub fn old_method(&self) -> OldResult {
        self.inner.new_method().into()
    }
}
```

## Code Review Checklist

- [ ] Are public APIs marked with appropriate stability attributes?
- [ ] Do breaking changes have deprecation warnings?
- [ ] Are new APIs tested for backward compatibility?
- [ ] Is documentation updated for API changes?
- [ ] Are migration guides provided for breaking changes?

## Enforcement

1. **Pre-commit hooks** run API stability checks
2. **CI pipelines** fail on unexpected API changes
3. **Code review** requires API stability sign-off
4. **Release process** includes API compatibility verification