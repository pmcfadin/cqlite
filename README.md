# CQLite

**A high-performance Rust library for local Apache Cassandra SSTable access**

[![Apache License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85+-red.svg)](https://www.rust-lang.org)
[![Cassandra](https://img.shields.io/badge/cassandra-5.0+-green.svg)](https://cassandra.apache.org)

> **Status**: M5 Complete (v0.9.0) - Core reading, CLI, Output Writers, Python and Node.js Bindings, and Write Support are production-ready

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

## Write Support

CQLite v0.9.0 (M5) ships write support across all interfaces: Rust core, Python,
Node.js, and CLI. Written data flushes to portable Cassandra 5.0 SSTables that
Cassandra can read directly via `nodetool refresh`.

The schema file below is included in the repository at
`test-data/schemas/write-test.cql`.

### Python

```python
import cqlite

# Open in writable mode — write_dir stores the WAL and flushed SSTables
with cqlite.open(
    'test-data/datasets/sstables',
    schema='test-data/schemas/write-test.cql',
    writable=True,
    write_dir='/tmp/my-writes',
) as db:
    db.execute(
        "INSERT INTO test_basic.simple_table (id, name, age) "
        "VALUES (11111111-1111-1111-1111-111111111111, 'Alice', 30)"
    )
    path = db.flush_run()
    print(f'Flushed SSTable: {path}')
```

### Node.js

```javascript
const { Database } = require('@cqlite/node');

const db = await Database.open('test-data/datasets/sstables', {
  schema: 'test-data/schemas/write-test.cql',
  writable: true,
  writeDir: '/tmp/my-writes',
});
await db.execute(
  "INSERT INTO test_basic.simple_table (id, name, age) " +
  "VALUES (22222222-2222-2222-2222-222222222222, 'Bob', 25)"
);
const path = await db.flushRun();
console.log('Flushed SSTable:', path);
await db.close();
```

### CLI

```bash
# Build with write support
cargo build --package cqlite-cli --features write-support

# Write via CQL INSERT
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/my-writes \
  --schema test-data/schemas/write-test.cql \
  --execute "INSERT INTO test_basic.simple_table (id, name, age) \
             VALUES (33333333-3333-3333-3333-333333333333, 'Carol', 28)"

# Flush memtable to SSTable
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/my-writes \
  --schema test-data/schemas/write-test.cql \
  --flush
```

See [docs/write-support.md](docs/write-support.md) for the full write guide,
including the Cassandra export workflow and known limitations.

## Feature Flags

`cqlite-core` gates optional functionality behind Cargo features. The table below
maps the public API you're likely to reach for to the feature that enables it.

| Want… | Enable feature | In defaults? |
|-------|----------------|--------------|
| Read / query path (`Database::open`, `execute`, `scan`, `get`) | `state_machine` | ✅ yes |
| Compression (LZ4 / Snappy / Deflate / Zstd) | `all-compression` | ✅ yes |
| Write path (`WriteEngine`, `Mutation`, `WriteEngine::write`/`flush`) | `write-support` | ✅ yes |
| `Database::flush` / `Database::compact` (high-level convenience) | `experimental` | ❌ opt-in |
| CLI ingestion / REPL helpers (`cqlite-cli`) | `cli-helpers` | ❌ opt-in |
| Performance metrics collection | `metrics` | ❌ opt-in |

Default features are `["all-compression", "state_machine", "write-support"]`
(see `cqlite-core/Cargo.toml`). `write-support` was folded into the defaults in
[#558](https://github.com/pmcfadin/cqlite/issues/558) — it gates only first-party
code and adds **no extra dependencies**, so read-only consumers pay nothing for it.
`flush`/`compact` on the high-level `Database` type remain behind `experimental`;
the equivalent engine-level `WriteEngine::flush` is part of `write-support`.

### Building with Custom Features

```bash
# Default build (read + write + compression)
cargo build

# Read-only consumer: drop the write path (still zero-cost to keep it, but explicit)
cargo build -p cqlite-core --no-default-features --features all-compression,state_machine

# Opt into high-level Database::flush / compact
cargo build -p cqlite-core --features experimental

# Minimal build (no compression, no query engine)
cargo build -p cqlite-core --no-default-features
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

### ✅ M5 Complete — v0.9.0 (May 2026)
- [x] Write support: WAL + memtable + flush to Cassandra SSTables
- [x] STCS compaction via `maintenance_step()`
- [x] Write API in Python, Node.js, and CLI
- [x] Full type coverage: Inet, Varint, Duration, Tuple, Frozen
- [x] E2E readback gate: write → flush → Cassandra `nodetool refresh` → verify

### 📋 Roadmap
- [ ] M6: WASM bindings for browser deployment
- [ ] M7: Performance validation + v1.0 release

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

### ✅ M5 Complete — v0.9.0 (May 2026)
- Write support: WAL-backed memtable + flush to portable Cassandra 5.0 SSTables
- STCS compaction (`maintenance_step()`)
- Write API exposed in Python (`flush_run`, `maintenance_step`, `write_stats`),
  Node.js (`flushRun`, `maintenanceStep`, `writeStats`), and CLI (`--writable`,
  `--write-dir`, `--flush`, `maintenance`, `write-stats`, `export-sstable`)
- Type roundtrips verified for all major types including Inet, Varint, Duration, Tuple, Frozen
- E2E validation against live Cassandra 5.0 (write → flush → `nodetool refresh` → `cqlsh`)

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

**Note**: M1 through M5 milestones are complete (v0.9.0). Core SSTable reading, CLI, output writers, Python bindings, Node.js bindings, and write support are production-ready. Next: M6 (WASM bindings) and M7 (performance validation + v1.0).