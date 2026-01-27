# CQLite

**A high-performance Rust library for local Apache Cassandra SSTable access**

[![Apache License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85+-red.svg)](https://www.rust-lang.org)
[![Cassandra](https://img.shields.io/badge/cassandra-5.0+-green.svg)](https://cassandra.apache.org)

> **Status**: M4 Complete - Core reading, CLI, Output Writers, Python and Node.js Bindings are production-ready

CQLite provides SQLite-like local access to Apache Cassandra SSTables, enabling developers to read Cassandra 5.0+ data files without cluster dependencies. Built in Rust for performance and safety.

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

# Run the CLI tool
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json
```

### Python

```bash
pip install cqlite-py
```

```python
import cqlite

with cqlite.open('path/to/sstables', schema='schema.cql') as db:
    for row in db.execute('SELECT * FROM keyspace.table LIMIT 5'):
        print(row.to_dict())
```

### Node.js

```bash
npm install @cqlite/node
```

```typescript
import { Database } from '@cqlite/node';

const db = await Database.open('path/to/sstables', { schema: 'schema.cql' });
const result = await db.execute('SELECT * FROM keyspace.table LIMIT 5');
for (const row of result.rows) {
  console.log(row.name);
}
await db.close();
```

## Feature Flags

CQLite uses Cargo feature flags to control optional functionality:

### Default Features (M1/M2 Scope)
- `all-compression` - All compression codecs (LZ4, Snappy, Deflate, Zstd)
- `state_machine` - Query engine (M2 CLI)

### Optional Features
- `benchmarks` - Performance benchmarks
- `tombstones` - Tombstone merging (M3+)
- `metrics` - Performance monitoring and telemetry

### Building with Custom Features

```bash
# Default build (M1/M2 features)
cargo build

# Build with metrics enabled
cargo build --features metrics

# Minimal build (no compression)
cargo build --no-default-features
```

## Features

### ✅ Complete (M1/M2)
- [x] Cassandra 5+ SSTable format parsing (100% of test tables)
- [x] All CQL types including collections and UDTs
- [x] All compression codecs (LZ4, Snappy, Deflate, Zstd)
- [x] CLI tool with REPL and one-shot query modes
- [x] SELECT with WHERE clause (partition/clustering key equality)
- [x] Output formats: Table, JSON, CSV

### ✅ M3 Complete (Jan 2026)
- [x] Parquet output format with Snappy compression
- [x] Export command (`cqlite export`)
- [x] Streaming export for large datasets
- [x] Output formats: CSV, JSON, Parquet, CQL

### ✅ M4 Complete (Jan 2026)
- [x] Python bindings with full CQL type support
- [x] Node.js bindings with TypeScript definitions
- [x] Streaming API for memory-efficient queries
- [x] pip/npm installable packages (5 platform builds each)
- [x] Type stubs for IDE support (Python mypy, TypeScript)

### 📋 Roadmap (M5+)
- [ ] Write support (SSTable creation)
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
# - Rust 1.85+

# Clone and build
git clone https://github.com/pmcfadin/cqlite.git
cd cqlite
cargo build

# Fetch test data (JSONL reference files are in git, SSTable binaries fetched separately)
bash test-data/scripts/fetch-datasets.sh

# Run tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core
```

### Contributing

1. **Check Issues**: Look for `good-first-issue` labels
2. **Discuss**: Join our community discussions
3. **Code**: Follow Rust best practices and include tests
4. **Test**: Ensure compatibility with real Cassandra data
5. **Document**: Update docs for user-facing changes

## Current Status

### ✅ M1 Complete (Dec 2025)
- All SSTable components parsed (Data.db, Index.db, Summary.db, Statistics.db, TOC)
- 33/33 test tables passing (100% validation)
- All 21 CQL primitive types + collections + UDTs + frozen types
- All compression algorithms working
- Tiered test coverage targets (see [PRD Section 5.1](docs/development/PRD.md#51--tiered-coverage-targets))

### ✅ M2 Complete (Jan 2026)
- CLI with one-shot and REPL modes
- SELECT queries with WHERE clause support
- Multiple output formats (Table, JSON, CSV)

### ✅ M3 Complete (Jan 2026)
- Parquet output format with Snappy compression
- Export command with CSV, JSON, Parquet, CQL formats
- Streaming export for memory-efficient large dataset handling
- Progress bar and statistics for exports

### ✅ M4 Complete (Jan 2026)
- Python bindings via PyO3 with sync-first API
- Node.js bindings via napi-rs with Promise-based API
- Full CQL type system (20+ types including collections, UDTs)
- Thread-safe database handles
- 500+ tests with 98%+ pass rate across both bindings

See [docs/development/PRD.md](docs/development/PRD.md) for milestone details.

## Technical Details

### Supported Formats
- **Cassandra 5.0+**: 'oa' format with BTI support
- **File Types**: Data.db, Index.db, Summary.db, Statistics.db
- **Compression**: LZ4, Snappy, Deflate

### Performance Targets
- **Parse Speed**: 1GB files in <10 seconds
- **Memory Usage**: <128MB for large SSTables
- **Query Latency**: Sub-millisecond partition lookups

### Language Bindings
- **Python**: Production-ready sync API (see [Python README](bindings/python/README.md))
- **Node.js**: Production-ready Promise API (see [Node.js README](bindings/node/README.md))
- **WASM**: Planned (M6+)

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

**Note**: M1 through M4 milestones are complete. Core SSTable reading, CLI, output writers, Python bindings, and Node.js bindings are production-ready. Next: M5 (Write Support).