# CQLite

**A high-performance Rust library for local Apache Cassandra SSTable access**

[![Apache License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70+-red.svg)](https://www.rust-lang.org)
[![Cassandra](https://img.shields.io/badge/cassandra-5.0+-green.svg)](https://cassandra.apache.org)

> 🚧 **Status**: Early Development - Not ready for production use

CQLite provides SQLite-like local access to Apache Cassandra SSTables, enabling developers to read and write Cassandra data files without cluster dependencies. Built in Rust for performance and safety, with bindings for Python, NodeJS, and WASM deployment.

## Vision

CQLite aims to become the standard tool for Cassandra SSTable manipulation outside of the main Apache Cassandra project, enabling new workflows for data analytics, migration, testing, and edge computing.

## Project Leadership

CQLite is designed by **Patrick McFadin**, Apache Cassandra PMC member with 13 years of Cassandra experience. The project embodies Apache Cassandra community values and will be donated to the Apache Cassandra project upon maturity.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/pmcfadin/cqlite.git
cd cqlite

# Build the project
cargo build --release

# Run the CLI tool (coming soon)
./target/release/cqlite parse my-table-data.db
```

## Features (Planned)

### ✅ Current (Alpha)
- [ ] Cassandra 5+ SSTable format parsing
- [ ] CQL type system support
- [ ] Basic CLI tool for testing

### 🚧 In Development  
- [ ] Complete CQL data type support
- [ ] Read operations with indexing
- [ ] Schema validation and evolution

### 📋 Roadmap
- [ ] Write operations with Cassandra 5 compatibility
- [ ] Python and NodeJS bindings
- [ ] WASM support for browser deployment
- [ ] Advanced query capabilities

## Architecture Highlights

**Simplified Design Philosophy:**
- **Single SSTable per table** - No compaction complexity
- **CQL parser** - Native CQL support using Antlr4 grammar
- **Cassandra 5+ focus** - Modern format support only
- **Memory efficient** - <128MB usage for large files
- **Zero dependencies** - Self-contained parsing engine

## Getting Involved

CQLite is developed in the open as an Apache-licensed project. We welcome contributions from the Cassandra community!

### Development Setup

```bash
# Prerequisites
# - Rust 1.70+
# - Docker (for test data generation)

# Clone and build
git clone https://github.com/pmcfadin/cqlite.git
cd cqlite
cargo build

# Run tests (requires Docker)
docker-compose up -d cassandra-test
cargo test
```

### Test Data Creation

We use real Cassandra 5 instances to generate test data:

```bash
# Start test environment
cd test-infrastructure
docker-compose up -d

# Generate test SSTables
cargo run --bin generate-test-data

# Validate parsing
cargo run --bin cqlite parse test-data/users-*.db
```

### Contributing

1. **Check Issues**: Look for `good-first-issue` labels
2. **Discuss**: Join our community discussions
3. **Code**: Follow Rust best practices and include tests
4. **Test**: Ensure compatibility with real Cassandra data
5. **Document**: Update docs for user-facing changes

## Current Status

### ✅ Completed
- Project architecture and design
- Test data generation strategy
- Development infrastructure setup

### 🔄 In Progress
- Core SSTable parsing engine
- CQL grammar integration
- CLI tool development

### 📅 Next Milestones
- **Week 4**: Basic parsing engine
- **Week 8**: CLI tool for user testing
- **Week 12**: Complete type system support
- **Week 16**: Read operations

See [MILESTONE_TRACKER.md](MILESTONE_TRACKER.md) for detailed progress.

## Technical Details

### Supported Formats
- **Cassandra 5.0+**: 'oa' format with BTI support
- **File Types**: Data.db, Index.db, Summary.db, Statistics.db
- **Compression**: LZ4, Snappy, Deflate

### Performance Targets
- **Parse Speed**: 1GB files in <10 seconds
- **Memory Usage**: <128MB for large SSTables
- **Query Latency**: Sub-millisecond partition lookups

### Language Bindings (Planned)
- **Python**: Pythonic API with asyncio support
- **NodeJS**: Modern JavaScript with TypeScript definitions
- **WASM**: Browser-compatible library
- **C API**: Foundation for additional languages

## Resources

- **Documentation**: [Complete project docs](docs/)
- **CQL Grammar**: [Patrick's Antlr4 CQL Grammar](https://github.com/pmcfadin/cassandra-antlr4-grammar)
- **Issues**: [GitHub Issues](https://github.com/pmcfadin/cqlite/issues)
- **Discussions**: [GitHub Discussions](https://github.com/pmcfadin/cqlite/discussions)

## Community

- **Slack**: `#cqlite` on ASF Slack
- **Mailing List**: dev@cassandra.apache.org (tag with [CQLite])
- **Weekly Sync**: Tuesdays 4pm UTC (calendar invite available)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

Special thanks to the Apache Cassandra community and the many contributors who make projects like this possible. CQLite builds on decades of database engineering innovation from the Cassandra project.

---

**Note**: This project is in early development. APIs and features are subject to change. We appreciate early feedback but recommend waiting for v0.1.0 for production evaluation.