# CQLite Development Environment

## Quick Start

```bash
# Build the project
cargo build

# Run tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core

# Run CLI
cargo run --package cqlite-cli -- --help

# Check code quality
cargo fmt --check
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
```

## Project Structure

```
cqlite/
├── cqlite-core/           # Core database engine
├── cqlite-cli/            # Command-line interface
├── bindings/
│   ├── python/            # Python bindings (PyO3)
│   └── node/              # Node.js bindings (napi-rs)
├── tools/                 # Development utilities
├── test-data/             # Test datasets and scripts
├── tests/                 # Integration tests
├── .github/workflows/     # CI/CD configuration
└── docs/                  # Documentation
```

## Development Workflow

### Prerequisites

- Rust 1.85+
- Python 3.9+ (for Python bindings)
- Node.js 18+ (for Node.js bindings)
- maturin (`pip install maturin`) for Python builds

### Common Tasks

```bash
# Building
cargo build                     # Debug build
cargo build --release           # Release build

# Testing
cargo test                      # All tests
cargo test --package cqlite-core  # Core library tests
cargo test --package cqlite-cli   # CLI tests

# Code quality
cargo fmt                       # Format code
cargo fmt --check               # Check formatting
cargo clippy                    # Lint check

# Python bindings
cd bindings/python
maturin develop                 # Development build
maturin build --release         # Release wheel
pytest tests -v                 # Run Python tests

# Node.js bindings
cd bindings/node
npm install && npm run build    # Build native module
npm test                        # Run tests
```

## Workspace Configuration

The project uses a Cargo workspace with shared dependencies:

### Core Dependencies
- **tokio**: Async runtime
- **serde**: Serialization framework
- **thiserror**: Error handling
- **tracing**: Structured logging

### CLI Dependencies
- **clap**: Command-line parsing
- **colored/indicatif**: Terminal UI
- **ratatui/crossterm**: TUI interface

### Storage Dependencies
- **lz4_flex**: LZ4 compression
- **snap**: Snappy compression
- **flate2**: Deflate compression
- **zstd**: Zstd compression

## Testing Strategy

### Unit Tests
Located in each crate's `src/` directory:
```bash
cargo test --package cqlite-core
```

### Integration Tests
Located in `tests/` directory:
```bash
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test
```

### Smoke Tests
Validate all 33 test tables:
```bash
bash test-data/scripts/smoke-test-all-tables.sh
```

## Continuous Integration

### GitHub Actions Workflows

1. **CI Pipeline** (`.github/workflows/ci.yml`)
   - Multi-platform testing (Linux, macOS, Windows)
   - Code formatting and linting
   - Coverage reporting

2. **Python CI** (`.github/workflows/python-ci.yml`)
   - Python binding tests across platforms

3. **Node.js CI** (`.github/workflows/node-ci.yml`)
   - Node.js binding tests across platforms

## Code Quality Standards

### Formatting
```bash
cargo fmt --all              # Format code
cargo fmt --all -- --check   # Check formatting
```

### Linting
```bash
env RUSTFLAGS="-D warnings" cargo clippy --all-targets --all-features
```

### Documentation
```bash
cargo doc --all-features --workspace --open
```

## IDE Configuration

### VS Code
Recommended extensions:
- rust-analyzer
- Even Better TOML
- CodeLLDB (for debugging)

Workspace settings in `.vscode/settings.json`:
```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.checkOnSave.command": "clippy"
}
```

## Debugging

### Debug Builds
```bash
cargo build                  # Debug build with symbols
cargo run --package cqlite-cli -- --verbose
```

### Logging
Set log level:
```bash
RUST_LOG=debug cargo run --package cqlite-cli
RUST_LOG=cqlite_core=trace cargo test
```

## Contributing

### Code Style
- Follow Rust standard formatting (`cargo fmt`)
- Use `clippy` recommendations
- Add documentation for public APIs
- Include tests for new functionality

### Commit Messages
Follow conventional commits:
```
feat: add query caching mechanism
fix: resolve memory leak in compaction
docs: update installation instructions
test: add integration tests for CLI
```

### Pull Request Process
1. Create feature branch
2. Implement changes with tests
3. Run `cargo fmt && cargo clippy && cargo test`
4. Submit PR with description
5. Address review feedback

## Troubleshooting

### Common Issues

**Build Failures:**
```bash
cargo clean && cargo build   # Clean and rebuild
```

**Test Failures:**
```bash
RUST_BACKTRACE=1 cargo test  # Full stack traces
```

**Missing test data:**
```bash
bash test-data/scripts/fetch-datasets.sh
```

## Additional Resources

- [CLAUDE.md](../../CLAUDE.md) - Development guidelines for AI assistants
- [SSTable Guide](../sstables-definitive-guide/README.md) - Format specification
- [Test Data Matrix](../../test-data/validation-matrix.md) - Test coverage
