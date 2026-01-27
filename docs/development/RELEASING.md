# Releasing CQLite Python Package

This guide provides step-by-step instructions for releasing the CQLite Python package (`cqlite-py`) to PyPI.

## Quick Release

For experienced operators (one-time setup already completed):

```bash
# Stable release (ensure pyproject.toml version matches!)
git tag -a v0.3.0 -m "Release v0.3.0"
git push origin v0.3.0

# Pre-release (for TestPyPI testing)
git tag -a v0.3.0-rc1 -m "Pre-release v0.3.0-rc1"
git push origin v0.3.0-rc1
```

## Prerequisites

Before releasing, ensure you have:

- **Repository admin access** (required for GitHub environment setup - contact @pmcfadin if needed)
- **PyPI account** with owner permissions on the `cqlite-py` project
- **Git push access** to create and push tags
- **Rust 1.85+** installed (required for Python bindings build)

## One-Time Setup (First Release Only)

These steps only need to be performed once before the first release.

### 1. Create GitHub Environments

GitHub environments enable secure OIDC authentication with PyPI.

1. Navigate to repository settings:
   ```
   https://github.com/pmcfadin/cqlite/settings/environments
   ```

2. Click **New environment** and create:
   - Environment name: `pypi`
   - No additional protection rules required (workflow handles this)

3. Repeat for TestPyPI:
   - Environment name: `testpypi`

### 2. Configure PyPI Trusted Publisher

This enables passwordless authentication via GitHub OIDC.

**Important**: For OIDC to work, the trusted publisher must be configured BEFORE the first release. PyPI supports "pending publishers" which allow you to configure the trusted publisher for a project that doesn't exist yet.

1. Log in to PyPI: https://pypi.org/

2. Create a "pending publisher" (for first-time setup):
   - Go to: https://pypi.org/manage/account/publishing/
   - Under "Add a new pending publisher", configure:
     - **PyPI project name**: `cqlite-py`
     - **Owner**: `pmcfadin`
     - **Repository name**: `cqlite`
     - **Workflow name**: `python-release.yml`
     - **Environment name**: `pypi`
   - Click **Add**
   - The project will be created automatically on first successful publish

3. For existing projects, add trusted publisher:
   - Navigate to: https://pypi.org/manage/project/cqlite-py/settings/publishing/
   - Click **Add a new publisher**
   - Configure:
     - **Owner**: `pmcfadin`
     - **Repository name**: `cqlite`
     - **Workflow name**: `python-release.yml`
     - **Environment name**: `pypi`
   - Click **Add**

### 3. Configure TestPyPI Trusted Publisher

Repeat the process for TestPyPI (used for pre-releases):

1. Log in to TestPyPI: https://test.pypi.org/

2. Navigate to: https://test.pypi.org/manage/project/cqlite-py/settings/publishing/

3. Add trusted publisher with same configuration:
   - **Owner**: `pmcfadin`
   - **Repository name**: `cqlite`
   - **Workflow name**: `python-release.yml`
   - **Environment name**: `testpypi`

## Release Checklist

### Pre-Release Validation

Before creating a release tag, verify:

- [ ] All tests pass: `pytest bindings/python/tests -v`
- [ ] Version updated in `bindings/python/pyproject.toml`
- [ ] Code quality checks pass:
  ```bash
  cargo fmt --check
  RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
  ```
- [ ] Python bindings build successfully: `cd bindings/python && maturin develop`

### Version Format

The version in `bindings/python/pyproject.toml` must follow semantic versioning:

```toml
[project]
version = "0.3.0"  # Stable release
# or
version = "0.4.0"  # Next version
```

## Stable Release Workflow

For production releases to PyPI (e.g., `v0.3.0`, `v1.0.0`):

### 1. Update Version

Edit `bindings/python/pyproject.toml`:
```toml
version = "0.3.0"
```

### 2. Commit Version Change (if needed)

```bash
git add bindings/python/pyproject.toml
git commit -m "chore(python): bump version to 0.3.0"
git push origin milestone4  # or your branch
```

### 3. Create and Push Tag

```bash
# Create annotated tag
git tag -a v0.3.0 -m "Release v0.3.0"

# Push tag to trigger release workflow
git push origin v0.3.0
```

### 4. Monitor GitHub Actions

1. Go to: https://github.com/pmcfadin/cqlite/actions/workflows/python-release.yml
2. Watch the triggered workflow
3. Verify all jobs complete successfully:
   - `build-sdist` - Source distribution
   - `build-wheels` - Platform wheels (5 platforms)
   - `publish-pypi` - PyPI publication
   - `github-release` - GitHub Release creation

### 5. Verify on PyPI

1. Check package page: https://pypi.org/project/cqlite-py/
2. Verify version is listed
3. Verify all 6 artifacts are present (1 sdist + 5 wheels)

## Pre-Release Workflow

For testing releases on TestPyPI (e.g., `v0.3.0-rc1`, `v0.3.0-alpha1`):

### Tag Patterns for Pre-Releases

| Pattern | Example | Destination |
|---------|---------|-------------|
| `v*-rc*` | `v0.3.0-rc1` | TestPyPI |
| `v*-alpha*` | `v0.3.0-alpha1` | TestPyPI |
| `v*-beta*` | `v0.3.0-beta1` | TestPyPI |

### Steps

```bash
# Create pre-release tag
git tag -a v0.3.0-rc1 -m "Pre-release v0.3.0-rc1"

# Push tag
git push origin v0.3.0-rc1
```

### Verify on TestPyPI

1. Check: https://test.pypi.org/project/cqlite-py/
2. Test installation (note: use `--extra-index-url` for dependencies):
   ```bash
   pip install --index-url https://test.pypi.org/simple/ \
               --extra-index-url https://pypi.org/simple/ \
               cqlite-py
   ```

**Note**: Pre-release tags do NOT enforce version consistency between the tag and `pyproject.toml`. This allows testing without committing version changes. Only stable releases (tags without `-`) validate version consistency.

## Post-Release Verification

After a successful release:

- [ ] Package visible on PyPI: https://pypi.org/project/cqlite-py/
- [ ] Installation works:
  ```bash
  pip install cqlite-py
  python -c "import cqlite; print(cqlite.__version__)"
  ```
- [ ] All 6 distribution files present:
  - 1 source distribution (`.tar.gz`)
  - 5 wheels (Linux x86_64, Linux ARM64, macOS x86_64, macOS ARM64, Windows x64)
- [ ] GitHub Release created with artifacts attached
- [ ] SHA256SUMS.txt included in release

## Troubleshooting

### OIDC Authentication Failures

**Symptom**: `publish-pypi` job fails with authentication error

**Solutions**:
1. Verify GitHub environment exists (`pypi` or `testpypi`)
2. Verify PyPI trusted publisher configuration matches exactly:
   - Owner: `pmcfadin`
   - Repository: `cqlite`
   - Workflow: `python-release.yml`
   - Environment: `pypi` or `testpypi`
3. Check that `id-token: write` permission is set in workflow

### Missing Artifacts

**Symptom**: Release fails with "Expected 5 wheels, found X"

**Solutions**:
1. Check individual `build-wheels` jobs for failures
2. Verify Rust toolchain is available for all targets
3. Check maturin version compatibility

### Version Mismatch Error

**Symptom**: `publish-pypi` fails with version mismatch

**Cause**: Tag version doesn't match `pyproject.toml` version

**Solution**:
1. Delete the tag: `git tag -d v0.3.0 && git push origin :refs/tags/v0.3.0`
2. Update `pyproject.toml` to match intended version
3. Commit and push
4. Re-create and push the tag

### Build Failures

**Symptom**: Wheel build fails for specific platform

**Solutions**:
1. Check platform-specific build logs
2. Verify manylinux compatibility (requires glibc 2.28+)
3. Check for platform-specific code issues

### First-Time OIDC Setup Issues

**Symptom**: "Project does not exist" or OIDC authentication fails on first release

**Solutions**:
1. Ensure you created a "pending publisher" (not just a trusted publisher)
2. Go to https://pypi.org/manage/account/publishing/ and add pending publisher
3. The project name, owner, repo, workflow, and environment must match EXACTLY
4. Double-check the environment name matches (`pypi` not `PyPI`)

### Tag Already Exists

**Symptom**: `git push` fails with "tag already exists on remote"

**Solution**:
```bash
# Delete remote tag and re-push
git push origin :refs/tags/v0.3.0
git push origin v0.3.0
```

### Cross-Compilation Failures (Linux ARM64)

**Symptom**: ARM64 Linux wheel build fails

**Solutions**:
1. Check that `cross` or QEMU is properly configured in CI
2. Verify maturin version supports cross-compilation
3. Check for architecture-specific Rust code issues

## Rollback / Yanking a Release

If a bad release is published to PyPI:

```bash
# Install twine if needed
pip install twine

# Yank the release (CAUTION: affects users immediately)
# Users with pinned versions can still install yanked releases
twine yank cqlite-py 0.3.0

# To un-yank (restore):
twine yank --undo cqlite-py 0.3.0
```

**Note**: Yanking is preferred over deletion. Yanked releases show a warning but remain installable for pinned dependencies.

## Platform Support

The release workflow builds wheels for:

| Platform | Target | Notes |
|----------|--------|-------|
| Linux x86_64 | `x86_64-unknown-linux-gnu` | manylinux_2_28 |
| Linux ARM64 | `aarch64-unknown-linux-gnu` | manylinux_2_28 |
| macOS x86_64 | `x86_64-apple-darwin` | macOS 14+ |
| macOS ARM64 | `aarch64-apple-darwin` | Apple Silicon |
| Windows x64 | `x86_64-pc-windows-msvc` | MSVC toolchain |

## How Tag Routing Works

The release workflow triggers on ANY `v*` tag push and routes based on tag format:

```
Tag pushed (v*)
    │
    ├─ Contains "-" (e.g., v0.3.0-rc1)
    │   └─► TestPyPI publish + GitHub Pre-release
    │
    └─ No "-" (e.g., v0.3.0)
        └─► PyPI publish + GitHub Release
```

Both paths create a GitHub Release with all artifacts attached.

## Reference

- **Release workflow**: `.github/workflows/python-release.yml`
- **CI workflow**: `.github/workflows/python-ci.yml`
- **Package config**: `bindings/python/pyproject.toml`
- **Strategic context**: `docs/development/PRD.md` Section 6.1
- **Technical details**: `docs/development/M4_spec.md` Section 6
