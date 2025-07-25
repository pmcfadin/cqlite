# CQLite Development Environment

## Quick Start

```bash
# Install development tools
just setup

# Build all targets
just build

# Run tests
just test

# Start development CLI
just dev-cli --help
```

## Project Structure

```
cqlite/
├── cqlite-core/           # Core database engine
├── cqlite-cli/            # Command-line interface
├── cqlite-ffi/            # C FFI bindings
├── cqlite-wasm/           # WebAssembly bindings
├── tests/                 # Integration tests and benchmarks
├── .github/workflows/     # CI/CD configuration
└── guides/               # Documentation and guides
```

## Development Workflow

### Prerequisites

- Rust 1.70+
- Just (task runner): `cargo install just`
- Node.js (for WASM testing)

### Common Tasks

```bash
# Development setup
just setup                    # Install tools and dependencies

# Building
just build                    # Build all targets
just build-release            # Build optimized release

# Testing
just test                     # Run all tests
just test-core               # Test only core library
just test-cli                # Test only CLI
just test-coverage           # Generate coverage report

# Code quality
just check                   # Run formatting and linting
just fix                     # Auto-fix formatting issues

# Development server
just watch                   # Auto-rebuild on changes
just dev-cli repl           # Start development CLI

# Specialized builds
just wasm                    # Build WASM package
just ffi                     # Build FFI library
```

## Workspace Configuration

The project uses a Cargo workspace with shared dependencies:

### Core Dependencies
- **tokio**: Async runtime
- **serde**: Serialization framework
- **anyhow/thiserror**: Error handling
- **tracing**: Structured logging

### CLI Dependencies
- **clap**: Command-line parsing
- **colored/indicatif**: Terminal UI
- **ratatui/crossterm**: Advanced TUI (optional)

### Storage Dependencies
- **lz4_flex**: Compression
- **memmap2**: Memory mapping
- **crc32fast**: Checksums

### Development Dependencies
- **criterion**: Benchmarking
- **proptest**: Property testing
- **tempfile**: Test utilities

## Testing Strategy

### Unit Tests
Located in each crate's `src/` directory:
```bash
cargo test --package cqlite-core
```

### Integration Tests
Located in `tests/` directory:
```bash
cargo test --package cqlite-integration-tests
```

### Benchmarks
```bash
just bench                   # All benchmarks
cargo bench --package cqlite-core  # Core benchmarks
```

### Property Tests
Using PropTest for fuzz testing:
```bash
cargo test --features proptest
```

## Continuous Integration

### GitHub Actions Workflows

1. **CI Pipeline** (`.github/workflows/ci.yml`)
   - Multi-platform testing (Linux, macOS, Windows)
   - Multiple Rust versions (stable, beta, nightly)
   - Code formatting and linting
   - Security audit
   - Coverage reporting

2. **Release Pipeline** (`.github/workflows/release.yml`)
   - Cross-platform binary builds
   - FFI library packaging
   - WASM package generation
   - Automated publishing to crates.io

### Local CI
Run the full CI pipeline locally:
```bash
just ci-local
```

## Code Quality Standards

### Formatting
```bash
cargo fmt --all              # Format code
cargo fmt --all -- --check   # Check formatting
```

### Linting
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Security
```bash
cargo audit                  # Security audit
```

### Documentation
```bash
cargo doc --all-features --workspace --open
```

## Performance Monitoring

### Benchmarks
Core performance benchmarks track:
- Storage operations (read/write/compaction)
- Query execution performance
- Memory usage patterns
- Concurrent operation throughput

### Profiling
```bash
just profile                 # CPU profiling with perf
just valgrind               # Memory profiling
```

## Cross-Platform Development

### Supported Targets
- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`
- `x86_64-pc-windows-msvc`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `wasm32-unknown-unknown`

### FFI Development
```bash
just ffi                     # Build C library
# Generated files:
# - target/release/libcqlite.{so,dylib,dll}
# - cqlite-ffi/include/cqlite.h
```

### WASM Development
```bash
just wasm                    # Build WASM package
# Generated files:
# - cqlite-wasm/pkg/cqlite_wasm.js
# - cqlite-wasm/pkg/cqlite_wasm_bg.wasm
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

### Neovim/Vim
Using nvim-lspconfig with rust-analyzer.

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

### GDB/LLDB
```bash
cargo build
gdb target/debug/cqlite-cli
(gdb) run --database test.db repl
```

## Release Process

### Version Management
Update version in all Cargo.toml files:
```bash
just release-prep 0.2.0
```

### Creating Releases
1. Update version numbers
2. Run full test suite: `just ci-local`
3. Create and push git tag: `git tag v0.2.0`
4. GitHub Actions will handle the rest

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
3. Run `just ci-local`
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
just test-coverage           # Check test coverage
RUST_BACKTRACE=1 cargo test  # Full stack traces
```

**FFI Issues:**
```bash
# Regenerate C headers
cd cqlite-ffi && cargo run --bin cbindgen_gen
```

**WASM Issues:**
```bash
# Reinstall wasm-pack
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
```

### Performance Issues
```bash
just profile                 # CPU profiling
just bench                   # Performance benchmarks
```

### Memory Issues
```bash
just valgrind               # Memory leak detection
cargo test --features debug-memory
```

## Additional Resources

- [Architecture Documentation](ARCHITECTURE.md)
- [API Specification](API_SPECIFICATION.md)
- [Technical Analysis](TECHNICAL_ANALYSIS.md)
- [Rust Development Guide](guides/workflows/development-workflow.md)
- [Testing Strategy](guides/workflows/testing-strategy.md)