# Contributing to CQLite

Thank you for your interest in contributing to CQLite! This document provides guidelines and information about contributing to the project.

## Table of Contents
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Issue Tracking](#issue-tracking)
- [Code Guidelines](#code-guidelines)
- [Testing](#testing)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)
- [Community](#community)

## Getting Started

### Prerequisites
- Rust 1.70.0 or later
- Git
- GitHub account
- Optional: Docker for testing with real Cassandra data

### Setting Up Development Environment

1. **Fork and Clone**
   ```bash
   git fork pmcfadin/cqlite
   git clone https://github.com/YOUR_USERNAME/cqlite.git
   cd cqlite
   ```

2. **Install Dependencies**
   ```bash
   cargo build
   cargo test
   ```

3. **Verify Installation**
   ```bash
   cargo run --bin cqlite -- --help
   ```

## Development Workflow

### Issue-Driven Development
We use GitHub issues to track all work. Please:

1. **Check existing issues** before creating new ones
2. **Comment on issues** you'd like to work on
3. **Create issues** for bugs, features, or improvements
4. **Reference issues** in commits and PRs

### Branch Strategy
- `main` - stable, production-ready code
- `develop` - integration branch (if needed for complex features)
- Feature branches: `feature/issue-number-short-description`
- Bug fix branches: `fix/issue-number-short-description`

### Commit Messages
Use conventional commit format:
```
type(scope): description

- feat: new feature
- fix: bug fix
- docs: documentation changes
- style: formatting changes
- refactor: code refactoring
- perf: performance improvements
- test: adding or updating tests
- chore: maintenance tasks
```

Examples:
```
feat(parser): add support for collections in WHERE clauses
fix(sstable): handle corrupted BTI index files gracefully
docs(readme): update installation instructions
```

## Issue Tracking

### Issue Types
We use structured issue templates:

- **Bug Report** - Report bugs or unexpected behavior
- **Feature Request** - Suggest new features or enhancements  
- **Performance Issue** - Report performance problems
- **Documentation** - Request documentation improvements

### Issue Labels
- **Priority**: `critical`, `high-priority`, `medium-priority`, `low-priority`
- **Type**: `bug`, `enhancement`, `documentation`, `performance`
- **Component**: `core`, `cli`, `storage`, `process`
- **Status**: `needs-triage`, `in-progress`, `blocked`

### Working on Issues
1. Comment on the issue to express interest
2. Wait for maintainer acknowledgment for large features
3. Create a branch from main: `git checkout -b feature/123-add-feature`
4. Reference the issue in commits: `git commit -m "feat: add feature (refs #123)"`

## Code Guidelines

### Rust Style
- Follow standard Rust formatting (`cargo fmt`)
- Use Clippy for linting (`cargo clippy`)
- Write idiomatic Rust code
- Use meaningful variable and function names
- Add doc comments for public APIs

### Architecture Principles
- **Modular Design**: Keep components loosely coupled
- **Error Handling**: Use `Result<T, E>` consistently
- **Performance**: Consider performance implications
- **Compatibility**: Maintain Cassandra compatibility
- **Testing**: Write testable code

### Code Organization
```
cqlite/
├── cqlite-core/     # Core SSTable reading and CQL parsing
├── cqlite-cli/      # Command-line interface
├── cqlite-ffi/      # Foreign function interface
├── cqlite-wasm/     # WebAssembly bindings
├── docs/            # Documentation
├── examples/        # Usage examples
└── tests/           # Integration tests
```

## Testing

### Test Types
1. **Unit Tests** - Test individual functions/modules
   ```bash
   cargo test
   ```

2. **Integration Tests** - Test component interactions
   ```bash
   cargo test --test integration
   ```

3. **CLI Tests** - Test command-line interface
   ```bash
   cargo test --package cqlite-cli
   ```

4. **Performance Tests** - Benchmark critical paths
   ```bash
   cargo bench
   ```

### Test Requirements
- All new code must have tests
- Bug fixes must include regression tests
- Performance changes should include benchmarks
- Tests should be deterministic and fast

### Test Data
- Use synthetic test data when possible
- Include real Cassandra SSTable files for compatibility testing
- Document test data sources and requirements

## Documentation

### Documentation Types
1. **Code Documentation** - Rust doc comments (`///`)
2. **API Documentation** - Generated with `cargo doc`
3. **User Documentation** - README, guides, examples
4. **Developer Documentation** - Architecture, contributing guides

### Documentation Standards
- Public APIs must have doc comments
- Include examples in doc comments when helpful
- Keep README.md up to date
- Update compatibility matrices when needed

## Pull Request Process

### Before Submitting
1. Ensure your code follows style guidelines
2. Add or update tests as appropriate
3. Update documentation if needed
4. Run the full test suite locally
5. Check that benchmarks don't regress significantly

### PR Requirements
1. **Link to Issue** - Reference the related issue(s)
2. **Clear Description** - Explain what changed and why
3. **Test Coverage** - Include appropriate tests
4. **Documentation** - Update docs if needed
5. **Compatibility** - Maintain Cassandra compatibility

### Review Process
1. Automated checks must pass (CI, formatting, tests)
2. At least one maintainer review required
3. Address review feedback promptly
4. Squash commits if requested
5. Maintain clean git history

### CI/CD Pipeline
Our GitHub Actions workflow includes:
- Multi-platform testing (Linux, macOS, Windows)
- Multiple Rust versions (stable, beta, nightly)
- Code formatting and linting checks
- Security auditing
- Performance benchmarking
- WASM and FFI builds

## Community

### Communication
- **GitHub Issues** - Bug reports, feature requests
- **GitHub Discussions** - Questions, ideas, general discussion
- **Pull Requests** - Code review and collaboration

### Code of Conduct
Please be respectful and constructive in all interactions. We're building a welcoming community focused on making CQLite better for everyone.

### Getting Help
- Check existing issues and documentation first
- Search GitHub discussions for similar questions
- Create a new discussion or issue if needed
- Be specific about your problem and environment

## Advanced Topics

### Architecture Overview
CQLite consists of several key components:

1. **SSTable Reader** (`cqlite-core/src/storage/sstable/`)
   - Handles binary SSTable format parsing
   - Supports multiple Cassandra versions
   - Includes compression and index handling

2. **CQL Parser** (`cqlite-core/src/parser/`)
   - Parses CQL queries and schema definitions
   - Pluggable backend system (nom, ANTLR)
   - AST generation and validation

3. **Query Engine** (`cqlite-core/src/query/`)
   - Executes SELECT queries against SSTable data
   - Handles filtering, sorting, and pagination
   - Schema-aware query planning

4. **CLI Interface** (`cqlite-cli/src/`)
   - Interactive and batch modes
   - Table scanning and query execution
   - Output formatting (table, JSON, CSV)

### Cassandra Compatibility
We maintain a compatibility matrix documenting:
- Supported Cassandra versions (3.x, 4.x, 5.x)
- SSTable format support
- CQL feature support
- Known limitations

Update `CASSANDRA_COMPATIBILITY_MATRIX.md` when adding support for new features or versions.

### Performance Considerations
- SSTable reading is I/O intensive - optimize for streaming
- Memory usage matters for large datasets
- CPU efficiency critical for query processing
- Benchmark performance-sensitive changes

## Release Process

Releases are automated through GitHub Actions:
1. Create a tag with `v*` format (e.g., `v0.4.0`)
2. GitHub Actions builds multi-platform binaries
3. Crates are published to crates.io
4. Release notes are generated automatically

For maintainers:
```bash
git tag v0.4.0
git push origin v0.4.0
```

## Questions?

If you have questions about contributing, please:
1. Check this guide and other documentation
2. Search existing issues and discussions
3. Create a new discussion with the "question" label
4. Be patient - maintainers will respond as time permits

Thank you for contributing to CQLite! 🚀