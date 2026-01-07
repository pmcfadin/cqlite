# Contributing to CQLite

Thank you for your interest in contributing to CQLite! This project aims to become the standard tool for Cassandra SSTable manipulation outside of the main Apache Cassandra project.

## Prerequisites

- **Rust 1.85+** (check with `rustc --version`)
- **Docker** (for test data generation)
- **Git**

## Development Setup

```bash
# Clone the repository
git clone https://github.com/pmcfadin/cqlite.git
cd cqlite

# Build the project
cargo build

# Set up test data environment variable
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets

# Run tests
cargo test --package cqlite-core
```

## Code Style

CQLite uses standard Rust tooling for code quality:

### Formatting

Code must be formatted with `rustfmt`. Configuration is in `.rustfmt.toml`.

```bash
cargo fmt
```

### Linting

Clippy must pass with warnings treated as errors (matching CI):

```bash
RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
```

Configuration is in `.clippy.toml` (MSRV: 1.85.0).

### Code Standards

- No `unwrap()` or `expect()` in library code (allowed in tests)
- Use `thiserror` for error types
- Memory target: <128MB for large files
- Prefer authoritative metadata over heuristics

## Testing

### Requirements

- Tests require real Cassandra 5.0 SSTable data
- Test data is located in `test-data/datasets/sstables/`
- Validation uses `sstabledump` output as reference

### Running Tests

```bash
# Core library tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core

# All workspace tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test

# Smoke test all tables
bash test-data/scripts/smoke-test-all-tables.sh
```

### Adding Tests

- Use real SSTable data, not synthetic test fixtures
- Validate parsing results against `sstabledump` output
- Place integration tests in the appropriate package's `tests/` directory

## Pull Request Process

1. **Create a feature branch** from `main`
2. **Make your changes** following the code style guidelines
3. **Run all checks locally:**
   ```bash
   cargo fmt
   RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
   env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test
   ```
4. **Fill out the PR template** (`.github/pull_request_template.md`)
5. **Link related issues** using "Closes #123" syntax
6. **Request review** and address feedback

### PR Checklist

- Code follows project style guidelines
- No new warnings from clippy
- Tests pass locally
- Documentation updated for user-facing changes
- Commits are clean and well-described

## Reporting Issues

### Bug Reports

Include:
- CQLite version and Rust version
- Operating system
- Steps to reproduce
- Expected vs actual behavior
- Relevant SSTable file details (if applicable)

### Feature Requests

- Check existing issues first
- Describe the use case clearly
- Explain how it fits with CQLite's goals

### Security Issues

For security vulnerabilities, please contact the maintainers directly rather than opening a public issue.

## Project Structure

```
cqlite-core/     # Core library (SSTable parsing, query engine)
cqlite-cli/      # Command-line interface
cqlite-ffi/      # C/C++ bindings
cqlite-wasm/     # WebAssembly bindings
test-data/       # Real Cassandra 5.0 SSTables for testing
```

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/pmcfadin/cqlite/issues)
- **Discussions**: [GitHub Discussions](https://github.com/pmcfadin/cqlite/discussions)
- **Documentation**: See `docs/` directory

## License

By contributing to CQLite, you agree that your contributions will be licensed under the Apache License 2.0.
