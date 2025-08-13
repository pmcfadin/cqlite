# CQLite Code Review Guidelines

## 🎯 Overview

These guidelines codify the structural and quality standards identified in the senior Rust engineer's review to ensure consistent, maintainable, and secure code.

## 🔴 MANDATORY REQUIREMENTS (Zero Tolerance)

### 1. Compilation & Basic Quality
- ✅ **MUST**: Code compiles without errors across all targets
- ✅ **MUST**: All tests pass (100% success rate required)
- ✅ **MUST**: Zero clippy warnings with `clippy::all` + `clippy::pedantic`
- ✅ **MUST**: Code formatted with `cargo fmt` (zero deviations)
- ✅ **MUST**: No security vulnerabilities (cargo audit clean)

### 2. Error Handling Standards
- ❌ **FORBIDDEN**: `unwrap()` or `expect()` in library code
- ✅ **REQUIRED**: Use `thiserror` for typed errors
- ✅ **REQUIRED**: Add context with `anyhow` on error boundaries
- ✅ **REQUIRED**: Proper error propagation with `?` operator

### 3. API Documentation
- ✅ **REQUIRED**: All public types and functions documented
- ✅ **REQUIRED**: Examples in documentation for complex APIs
- ✅ **REQUIRED**: Feature gates documented where applicable
- ✅ **REQUIRED**: Safety invariants documented for unsafe code

## 🟡 CODE QUALITY STANDARDS

### 1. Clarity and Readability
```rust
// ✅ GOOD: Descriptive names, early returns
fn process_user_request(request: &UserRequest) -> Result<Response, ApiError> {
    if request.is_empty() {
        return Err(ApiError::EmptyRequest);
    }
    
    let user = authenticate_user(&request.token)?;
    if !user.has_permission(Permission::Read) {
        return Err(ApiError::Unauthorized);
    }
    
    // Main logic here...
    Ok(response)
}

// ❌ BAD: Deep nesting, unclear names
fn proc_req(req: &Req) -> Result<Resp, Err> {
    if !req.is_empty() {
        if let Ok(u) = auth(req.token) {
            if u.can_read() {
                // nested logic...
            } else {
                Err(Err::Unauth)
            }
        } else {
            Err(Err::AuthFail)
        }
    } else {
        Err(Err::Empty)
    }
}
```

**Standards**:
- Maximum nesting depth: 3 levels
- Prefer early returns and guard clauses
- Use descriptive variable and function names
- Functions should do one thing well

### 2. Async/Concurrency Best Practices
```rust
// ✅ GOOD: Proper async boundaries
async fn fetch_user_data(id: UserId) -> Result<UserData, DatabaseError> {
    let data = database_client.get_user(id).await?;
    Ok(data)
}

// ❌ BAD: Blocking in async context
async fn bad_fetch_user_data(id: UserId) -> Result<UserData, DatabaseError> {
    let data = std::thread::sleep(Duration::from_secs(1)); // BLOCKING!
    database_client.get_user(id).await
}
```

**Standards**:
- Never block async runtime with synchronous I/O
- Validate `Send`/`Sync` boundaries explicitly
- Scope Tokio features precisely (no "full" features)
- Document thread safety guarantees

### 3. Performance Considerations
```rust
// ✅ GOOD: Avoid unnecessary allocations
fn process_items(items: &[Item]) -> Vec<ProcessedItem> {
    items.iter()
        .filter_map(|item| item.process())
        .collect()
}

// ❌ BAD: Unnecessary clones and allocations
fn bad_process_items(items: &[Item]) -> Vec<ProcessedItem> {
    let mut result = Vec::new();
    for item in items.clone() { // unnecessary clone
        if let Some(processed) = item.clone().process() { // another clone
            result.push(processed);
        }
    }
    result
}
```

**Standards**:
- Use `criterion` for performance-critical benchmarks
- Avoid unnecessary allocations and clones
- Profile before optimizing
- Document performance characteristics

## 🟢 PROJECT STRUCTURE STANDARDS

### 1. Workspace Organization
```toml
# ✅ GOOD: Clean workspace structure
[workspace]
members = [
    "cqlite-core",      # Core database engine
    "cqlite-cli",       # Command-line interface
    "cqlite-ffi",       # Foreign function interface
    "cqlite-wasm",      # WebAssembly bindings
    "tests",            # Integration tests
    "examples",         # Usage examples
    "tools/*",          # Development tools
]

# Consistent dependency inheritance
[workspace.dependencies]
tokio = { version = "1.0", default-features = false }
serde = { version = "1.0", features = ["derive"] }
```

**Standards**:
- All crates inherit workspace properties where possible
- No root `src/lib.rs` without `[package]` section
- Logical separation of concerns across crates
- Consistent versioning and metadata

### 2. Dependency Management
```toml
# ✅ GOOD: Feature-gated optional dependencies
[dependencies]
lz4_flex = { version = "0.11", optional = true }
snap = { version = "1.1", optional = true }
tokio = { workspace = true, features = ["fs", "io-util"] }

[features]
default = ["lz4"]
lz4 = ["dep:lz4_flex"]
snappy = ["dep:snap"]
all-compression = ["lz4", "snappy"]

# ❌ BAD: Monolithic dependencies
[dependencies]
tokio = { version = "1.0", features = ["full"] }  # Too broad
```

**Standards**:
- Minimize dependencies and pin via workspace
- Feature-gate optional functionality
- Avoid "full" feature sets
- Regular security auditing with `cargo audit`

### 3. Lint Configuration
```toml
# ✅ GOOD: Properly organized lints
[workspace.lints.rust]
unused_imports = "deny"
unused_variables = "deny"
dead_code = "warn"
warnings = "warn"

[workspace.lints.clippy]
all = { level = "warn", priority = -1 }
pedantic = { level = "warn", priority = -1 }
missing_docs_in_private_items = "warn"
```

**Standards**:
- Separate rustc and clippy lints correctly
- Use appropriate priority levels
- Zero tolerance for unused code patterns
- Consistent warning levels across workspace

## 🧪 TESTING REQUIREMENTS

### 1. Test Coverage Standards
```rust
// ✅ GOOD: Comprehensive test coverage
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    
    #[test]
    fn test_basic_functionality() {
        // Unit test for happy path
    }
    
    #[test]
    fn test_error_conditions() {
        // Test error handling
    }
    
    proptest! {
        #[test]
        fn test_property_based(input in any::<ValidInput>()) {
            // Property-based testing
        }
    }
}
```

**Standards**:
- Unit tests for all public APIs
- Integration tests for cross-crate functionality
- Property-based tests for complex logic
- Deterministic and fast test execution
- Minimum 15% code coverage (CI enforced)

### 2. Test Organization
```
tests/
├── integration/           # Cross-crate integration tests
├── benchmarks/           # Performance benchmarks
├── fixtures/             # Test data and helpers
└── end-to-end/          # Full system tests
```

**Standards**:
- Logical test organization by scope
- Reusable test fixtures and helpers
- Performance regression testing
- Automated test execution in CI

## 🛡️ SECURITY STANDARDS

### 1. Dependency Security
```bash
# Required CI checks
cargo audit               # Security vulnerability scanning
cargo deny check         # License and policy compliance
```

**Standards**:
- All dependencies regularly audited
- Clear licensing policy (MIT OR Apache-2.0)
- No known security vulnerabilities
- Minimal dependency surface area

### 2. Code Security
```rust
// ✅ GOOD: Safe memory management
fn process_buffer(data: &[u8]) -> Result<ProcessedData, Error> {
    // Bounds checking handled by Rust
    let chunk = &data[..min(data.len(), MAX_CHUNK_SIZE)];
    process_chunk(chunk)
}

// ❌ BAD: Unsafe without justification
unsafe fn bad_process_buffer(ptr: *const u8, len: usize) -> ProcessedData {
    // Unsafe without safety documentation
    let slice = std::slice::from_raw_parts(ptr, len);
    // ...
}
```

**Standards**:
- Minimize unsafe code usage
- Document safety invariants for all unsafe blocks
- No hardcoded secrets or credentials
- Proper input validation and sanitization

## 🔧 TOOLING AND AUTOMATION

### 1. Required Toolchain Configuration
```toml
# rust-toolchain.toml
[toolchain]
channel = "stable"
components = ["clippy", "rustfmt"]
targets = ["wasm32-unknown-unknown"]
```

### 2. CI/CD Pipeline Requirements
- **Format Check**: `cargo fmt --all --check`
- **Lint Check**: `cargo clippy --all-targets --all-features -- -D warnings`
- **Test Execution**: `cargo test --all-features --workspace`
- **Security Audit**: `cargo audit`
- **Dependency Policy**: `cargo deny check`
- **WASM Build**: `wasm-pack build --target web`

### 3. Pre-commit Hooks
All developers must use pre-commit hooks that enforce:
- Code formatting
- Lint checking
- Basic test execution
- Project structure validation

## 📝 REVIEW PROCESS

### 1. Reviewer Checklist
- [ ] Code compiles and tests pass
- [ ] No clippy warnings
- [ ] Proper error handling (no unwrap/expect)
- [ ] Public APIs documented
- [ ] Security considerations addressed
- [ ] Performance implications considered
- [ ] Test coverage adequate

### 2. Review Priority Levels
- **P0 - Critical**: Security issues, compilation errors, test failures
- **P1 - High**: API design, error handling, performance issues
- **P2 - Medium**: Code clarity, documentation, test coverage
- **P3 - Low**: Style preferences, minor optimizations

### 3. Approval Requirements
- At least one senior developer approval
- All CI checks passing
- No outstanding P0 or P1 issues
- Documentation updated if needed

## 🚀 CONTINUOUS IMPROVEMENT

### 1. Metrics Tracking
- Code coverage trends
- Build time progression
- Test execution time
- Security vulnerability count
- Dependency freshness

### 2. Regular Reviews
- Monthly dependency updates
- Quarterly security audits
- Bi-annual toolchain updates
- Annual guideline reviews

## 📚 REFERENCES

- [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [Rust Security Guidelines](https://anssi-fr.github.io/rust-guide/)
- [Clippy Lint Reference](https://rust-lang.github.io/rust-clippy/)

---

*These guidelines are enforced through automated tooling and are subject to regular updates based on project needs and Rust ecosystem evolution.*