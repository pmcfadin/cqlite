# Pre-Push Validation Checklist

Complete step-by-step validation before pushing code.

## Quick Start

```bash
# Run automated validation
cd /Users/patrick/local_projects/cqlite
./scripts/ci/validate-cleanup.sh
```

## Manual Validation (Step-by-Step)

### Step 1: Code Formatting

**Command:**
```bash
cargo fmt --all
```

**What it checks:**
- Consistent code style
- Indentation
- Line length
- Import organization

**If changes made:**
```bash
git add -u
git commit -m "style: cargo fmt"
```

**Expected:** No changes (code already formatted) or commit created

---

### Step 2: Clippy Linting

**Command:**
```bash
cargo clippy --package cqlite-core --lib --all-features -- -D warnings
```

**Flags:**
- `-D warnings`: Treat warnings as errors
- `--all-features`: Check with all features enabled

**What it checks:**
- Code quality issues
- Common mistakes
- Performance anti-patterns
- Idiomatic Rust usage

**Common warnings:**
```
warning: unused import: `std::collections::HashMap`
warning: this expression creates a reference which is immediately dereferenced
warning: redundant clone
```

**Fix warnings immediately** - don't proceed until zero warnings.

**Expected:** Exit code 0, no warnings

---

### Step 3: Build (Minimal Features)

**Command:**
```bash
cargo build --package cqlite-core \
    --no-default-features \
    --features=all-compression
```

**What it checks:**
- M1 scope builds correctly
- Feature gates work
- No unexpected dependencies

**Expected:** Build succeeds

**If fails:**
- Check feature gates in `Cargo.toml`
- Verify conditional compilation (`#[cfg(...)]`)
- Check dependency features

---

### Step 4: Build (All Features)

**Command:**
```bash
cargo build --package cqlite-core --all-features
```

**What it checks:**
- Complete feature set builds
- Optional features compile
- No feature conflicts

**Expected:** Build succeeds

---

### Step 5: Test (Library)

**Command:**
```bash
cargo test --package cqlite-core --lib --all-features
```

**What it checks:**
- Unit tests pass
- All code paths work
- No regressions

**Track:**
```
running 147 tests
test result: ok. 147 passed; 0 failed; 0 ignored
```

**Expected:**
- All tests pass
- Test count doesn't decrease unexpectedly

---

### Step 6: Test (Integration)

**Command:**
```bash
cargo test --test '*'
```

**What it checks:**
- End-to-end scenarios
- Real SSTable parsing
- Integration with test data

**Expected:** All integration tests pass

---

### Step 7: Doc Tests

**Command:**
```bash
cargo test --doc --package cqlite-core
```

**What it checks:**
- Documentation examples compile
- Example code correct

**Expected:** Doc tests pass

---

### Step 8: Verify No Unused Imports

**Command:**
```bash
# Check for unused imports
cargo clippy --package cqlite-core --lib --all-features -- \
    -W unused-imports
```

**Expected:** No unused imports

---

### Step 9: Build Documentation

**Command:**
```bash
cargo doc --no-deps --package cqlite-core --all-features
```

**What it checks:**
- Documentation builds
- No broken links
- Examples compile

**Expected:** Documentation builds successfully

---

### Step 10: Check for Dead Code

**Command:**
```bash
cargo clippy --package cqlite-core --lib --all-features -- \
    -W dead-code
```

**Expected:**
- For cleanup issues: Some dead code warnings expected (documenting)
- For feature work: No dead code

---

### Step 11: Coverage Check (Optional)

**Command:**
```bash
cargo tarpaulin --package cqlite-core --out Html --output-dir coverage/
```

**What it checks:**
- Test coverage percentage
- Uncovered lines

**Target:** ≥90% (PRD requirement for M1)

**View report:**
```bash
open coverage/index.html
```

---

## Automated Script

The `scripts/ci/validate-cleanup.sh` script runs steps 1-6 automatically:

```bash
#!/bin/bash
set -euo pipefail

echo "=== CQLite Validation ==="

echo "[1/6] Formatting..."
cargo fmt --all -- --check

echo "[2/6] Clippy (zero warnings)..."
cargo clippy --package cqlite-core --lib --all-features -- -D warnings

echo "[3/6] Build (minimal features)..."
cargo build --package cqlite-core \
    --no-default-features \
    --features=all-compression

echo "[4/6] Build (all features)..."
cargo build --package cqlite-core --all-features

echo "[5/6] Test (library)..."
cargo test --package cqlite-core --lib --all-features

echo "[6/6] Test (integration)..."
cargo test --test '*'

echo "✅ All validations passed!"
```

---

## Validation Matrix

| Check | Command | Time | Required |
|-------|---------|------|----------|
| Format | `cargo fmt` | 1s | Yes |
| Clippy | `cargo clippy` | 30s | Yes |
| Build (min) | `cargo build --no-default...` | 60s | Yes |
| Build (all) | `cargo build --all-features` | 60s | Yes |
| Test (lib) | `cargo test --lib` | 45s | Yes |
| Test (int) | `cargo test --test '*'` | 120s | Yes |
| Doc test | `cargo test --doc` | 30s | Optional |
| Coverage | `cargo tarpaulin` | 240s | Optional |

**Total time (required):** ~5 minutes

---

## Feature Flag Validation

Test multiple feature combinations:

```bash
#!/bin/bash

features=(
    "all-compression"
    "all-compression,benchmarks"
    "all-features"
)

for feature_set in "${features[@]}"; do
    echo "Testing features: $feature_set"
    
    cargo test --package cqlite-core \
        --no-default-features \
        --features="$feature_set"
    
    if [ $? -ne 0 ]; then
        echo "❌ Failed with features: $feature_set"
        exit 1
    fi
done

echo "✅ All feature combinations validated"
```

---

## Platform-Specific Checks

### Linux
```bash
# Standard validation
./scripts/ci/validate-cleanup.sh
```

### macOS
```bash
# Same as Linux
./scripts/ci/validate-cleanup.sh
```

### Windows
```powershell
# PowerShell equivalent
cargo fmt --all
cargo clippy --package cqlite-core --lib --all-features -- -D warnings
cargo test --package cqlite-core --lib --all-features
```

---

## Troubleshooting

### Clippy Errors

**Error:** "unused import"
```
warning: unused import: `std::collections::HashMap`
  --> src/module.rs:12:5
```

**Fix:** Remove unused import

---

**Error:** "needless borrow"
```
warning: this expression creates a reference which is immediately dereferenced
  --> src/module.rs:42:10
```

**Fix:** Remove unnecessary `&`

---

### Build Errors

**Error:** "cannot find function in this scope"
```
error[E0425]: cannot find function `parse_row` in this scope
```

**Fix:** Check feature gates, ensure function is available

---

**Error:** "feature X is required"
```
error: feature `benchmarks` is required
```

**Fix:** Add feature flag to build command

---

### Test Failures

**Error:** Test assertion fails
```
thread 'test_parse_simple_row' panicked at 'assertion failed: `(left == right)`
```

**Fix:**
1. Run with `--nocapture` to see output
2. Debug with `RUST_LOG=debug`
3. Fix logic error

---

## CI vs Local

### Differences

CI may have:
- Different Rust version
- Different platform
- Different environment variables
- Different file paths

### Reproduce CI Locally

```bash
# Check CI Rust version
cat .github/workflows/rust.yml | grep rust-version

# Install same version
rustup install 1.70.0
rustup default 1.70.0

# Run validation
./scripts/ci/validate-cleanup.sh
```

---

## Summary Checklist

Use this for quick reference:

- [ ] `cargo fmt` clean
- [ ] `cargo clippy` zero warnings
- [ ] Minimal features build
- [ ] All features build
- [ ] Library tests pass
- [ ] Integration tests pass
- [ ] No unused imports
- [ ] Documentation builds
- [ ] Coverage ≥90% (if checking)

**If all checked:** Ready to push! 🚀

---

## Next Steps

After passing validation:

1. **Commit changes**
   ```bash
   git add -A
   git commit -m "feat: implement feature X"
   ```

2. **Push branch**
   ```bash
   git push origin feature/my-feature
   ```

3. **Create PR**
   ```bash
   gh pr create --title "feat: Feature X" --body "..."
   ```

4. **Monitor CI**
   ```bash
   gh run watch
   ```

5. **Merge when green** ✅

