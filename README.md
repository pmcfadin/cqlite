<p align="center">
  <img src="website/src/assets/cqlite.png" alt="CQLite" width="480">
</p>

<p align="center"><strong>A high-performance Rust library for local Apache Cassandra SSTable access</strong></p>

<p align="center">
  <a href="https://github.com/pmcfadin/cqlite/actions/workflows/ci.yml"><img src="https://github.com/pmcfadin/cqlite/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://crates.io/crates/cqlite-cli"><img src="https://img.shields.io/crates/v/cqlite-cli.svg?label=crates.io%20cqlite-cli" alt="crates.io"></a>
  <a href="https://docs.rs/cqlite-core"><img src="https://img.shields.io/docsrs/cqlite-core.svg?label=docs.rs" alt="docs.rs"></a>
  <a href="https://pypi.org/project/cqlite-py/"><img src="https://img.shields.io/pypi/v/cqlite-py.svg?label=pypi%20cqlite-py" alt="PyPI"></a>
  <a href="https://www.npmjs.com/package/@cqlite/node"><img src="https://img.shields.io/npm/v/@cqlite/node.svg?label=npm%20%40cqlite%2Fnode" alt="npm"></a>
  <a href="https://pmcfadin.github.io/cqlite/"><img src="https://img.shields.io/badge/docs-pmcfadin.github.io%2Fcqlite-blue.svg" alt="Docs"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="Apache License"></a>
  <a href="https://www.rust-lang.org"><img src="https://img.shields.io/badge/rust-1.85+-red.svg" alt="Rust"></a>
  <a href="https://cassandra.apache.org"><img src="https://img.shields.io/badge/cassandra-5.0+-green.svg" alt="Cassandra"></a>
</p>

> **Status**: v0.12.0 — Core reading, CLI, output writers, Python & Node.js bindings, and write support are production-ready, now with **byte-for-byte compaction parity against Apache Cassandra**, an Arrow Flight + Trino connector, canonical BTI (`da`) write/read, and CDC-style delta export. See [CHANGELOG.md](CHANGELOG.md).

CQLite provides SQLite-like local access to Apache Cassandra SSTables, enabling developers to read Cassandra 5.0+ data files without cluster dependencies. Built in Rust for performance and safety.

> ⭐ **Find CQLite useful?** [**Star the repo**](https://github.com/pmcfadin/cqlite) — it is the clearest signal that this work matters and directly drives how much time goes into it.
> 🐛 **Hit a bug or need a feature?** [**Open an issue**](https://github.com/pmcfadin/cqlite/issues/new/choose). For questions and ideas, use [Discussions](https://github.com/pmcfadin/cqlite/discussions). See [Known Issues](#known-issues) and the [Roadmap](#roadmap) before filing.

## Documentation

Full documentation is at **[https://pmcfadin.github.io/cqlite/](https://pmcfadin.github.io/cqlite/)**:

| Section | URL |
|---------|-----|
| User Docs — install, quick start, CLI, Python, Node.js | [/cqlite/user-docs/](https://pmcfadin.github.io/cqlite/user-docs/) |
| SSTable Format Guide — binary format deep-dive | [/cqlite/sstable-format/](https://pmcfadin.github.io/cqlite/sstable-format/) |
| For Agents: Using CQLite — LLM/agent integration | [/cqlite/agents-using/](https://pmcfadin.github.io/cqlite/agents-using/) |
| For Agents: Developing CQLite — contributor doctrine, gate contract | [/cqlite/agents-developing/](https://pmcfadin.github.io/cqlite/agents-developing/) |

## Vision

CQLite aims to become the standard tool for Cassandra SSTable manipulation outside of the main Apache Cassandra project, enabling new workflows for data analytics, migration, testing, and edge computing.

## Project Leadership

CQLite is designed by **Patrick McFadin**, Apache Cassandra PMC member with over a decade of Cassandra experience. The project embodies Apache Cassandra community values and will be donated to the Apache Cassandra project upon maturity.

## Install

### CLI (Homebrew — macOS + Linux)

The quickest path on macOS (Apple Silicon or Intel) and Linux (x86_64 or arm64).
The formula verifies the release checksum before installing:

```bash
brew install pmcfadin/cqlite/cqlite
cqlite --help
```

### CLI (from crates.io — requires Rust 1.85+)

```bash
cargo install cqlite-cli      # installs the `cqlite` binary
cqlite --help
```

### CLI (prebuilt binaries — no Rust toolchain required)

Each [GitHub release](https://github.com/pmcfadin/cqlite/releases) attaches a
prebuilt `cqlite` CLI binary for the common platforms, each with a `.sha256`
checksum sidecar:

| Platform | Asset |
|----------|-------|
| macOS (Apple Silicon) | `cqlite-aarch64-apple-darwin.tar.gz` |
| macOS (Intel) | `cqlite-x86_64-apple-darwin.tar.gz` |
| Linux x86_64 (glibc) | `cqlite-x86_64-unknown-linux-gnu.tar.gz` |
| Linux x86_64 (static musl) | `cqlite-x86_64-unknown-linux-musl.tar.gz` |
| Linux arm64 (glibc) | `cqlite-aarch64-unknown-linux-gnu.tar.gz` |
| Windows x86_64 | `cqlite-x86_64-pc-windows-gnu.zip` |

```bash
# Example: macOS Apple Silicon
TARGET=aarch64-apple-darwin
curl -fsSLO https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-$TARGET.tar.gz
curl -fsSLO https://github.com/pmcfadin/cqlite/releases/latest/download/cqlite-$TARGET.tar.gz.sha256
shasum -a 256 -c cqlite-$TARGET.tar.gz.sha256   # verify (use sha256sum -c on Linux)
tar xzf cqlite-$TARGET.tar.gz
./cqlite --help
```

### Rust library

```bash
cargo add cqlite-core         # use cqlite-core as a dependency
```

See [Using cqlite-core as a dependency](docs/using-cqlite-core-as-a-dependency.md) and the [API docs](https://docs.rs/cqlite-core).

### Language bindings

```bash
pip install cqlite-py        # Python
npm install @cqlite/node     # Node.js
```

### Arrow Flight server (container)

Query a Cassandra node's SSTables over Arrow Flight (gRPC) with the
`cqlite-flight` server, published as a multi-arch image on every release tag.
Mount the data dir read-only and point `--data-dir` at it:

```bash
docker run --rm -p 8815:8815 \
  -v /var/lib/cassandra:/var/lib/cassandra:ro \
  ghcr.io/pmcfadin/cqlite-flight:latest \
  --data-dir /var/lib/cassandra/data --listen 0.0.0.0:8815
```

See [`cqlite-flight/README.md`](cqlite-flight/README.md) for image tags, the
ticket/predicate API, and the [`trino-connector`](trino-connector) that builds
on it.

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
including the Cassandra export workflow and known limitations. To embed
`cqlite-core` in your own Rust project (dependency line, feature flags, and a
compiling write example), see
[docs/using-cqlite-core-as-a-dependency.md](docs/using-cqlite-core-as-a-dependency.md).

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

### ✅ Since v0.9.0 (v0.10 → v0.11.0, Jun 2026)
- [x] Embeddable Parquet writer in `cqlite-core` (behind a `parquet` feature) + `export_parquet` in Python/Node
- [x] Version-gated reads for the Cassandra 5.0 `oa` format; graceful handling of `da` (BTI)
- [x] Real BTI trie node-type dispatch and schema-typed query result columns
- [x] Published documentation site at [pmcfadin.github.io/cqlite](https://pmcfadin.github.io/cqlite/)

### ✅ v0.12.0 (Jun 2026) — the compaction release
- [x] **Byte-for-byte compaction parity vs Apache Cassandra** — `cqlite compact` + a differential harness in CI, full reconciliation rule set (complex deletions, tombstone tie-breaks, `gc_grace` purging, range tombstones, per-cell/dropped-column purging, non-frozen UDT multi-cell)
- [x] **Arrow Flight server + Trino connector** — query SSTables as a federated source with predicate, token-range, and aggregation pushdown
- [x] **Canonical BTI (`da`) write + end-to-end read** — emit Cassandra-format trie-indexed SSTables
- [x] **CDC-style delta-scan / `delta-export`** — project SSTable generations to Parquet envelopes with full tombstone fidelity
- [x] **`WRITETIME()` / `TTL()` in `SELECT`** and query-engine completeness (`PER PARTITION LIMIT`, static columns, clustering order/bounds, partition-targeted lookups)
- [x] crates.io OIDC trusted publishing + Homebrew tap
- See [CHANGELOG.md](CHANGELOG.md) for the full per-release detail

### 📋 Roadmap

See the [**Roadmap**](#roadmap) section below for in-flight epics and milestones.

## Roadmap

CQLite is at **v0.12.0** and production-ready for the use cases above. The path to
**v1.0** is tracked in the open. Full detail, with milestones, lives at
[pmcfadin.github.io/cqlite → Roadmap](https://pmcfadin.github.io/cqlite/user-docs/roadmap/).

| Workstream | Epic |
|------------|------|
| Wire storage-layer capabilities (bloom/index/BTI seeks) into the CQL query path + regression guards | [#951](https://github.com/pmcfadin/cqlite/issues/951) |
| Read-path performance & I/O backend (parallel single-reader scans, io_uring spike) | [#906](https://github.com/pmcfadin/cqlite/issues/906) |
| CLI & bindings polish (DX & cleanup) | [#907](https://github.com/pmcfadin/cqlite/issues/907) |
| Compaction byte-parity follow-ups (range tombstones e2e + edge cases) | [#938](https://github.com/pmcfadin/cqlite/issues/938) |
| M6 — WASM bindings · M7 — performance validation + **v1.0** | _planned_ |

The roadmap follows real-world use. Want something prioritized?
[Open or 👍 an issue](https://github.com/pmcfadin/cqlite/issues) — and
[⭐ star the repo](https://github.com/pmcfadin/cqlite).

## Known Issues

CQLite is honest about its sharp edges. The current release (`v0.12.0`) has a few
known gaps — none of which block the core read/export workflows. Full, dated list:
[pmcfadin.github.io/cqlite → Known Issues](https://pmcfadin.github.io/cqlite/user-docs/known-issues/).

| Issue | Impact | Tracking |
|-------|--------|----------|
| `SET<FROZEN<UDT>>` fails to deserialize in the Python bindings | Python only; CLI/Rust unaffected | [#804](https://github.com/pmcfadin/cqlite/issues/804) |
| Concurrent queries on one `Database` can race (`Column not found`) | Use one handle per thread | [#805](https://github.com/pmcfadin/cqlite/issues/805) |
| Wide partitions written by CQLite scan linearly (`promoted_index_length = 0`) | Perf on 10k+ rows/partition | [#751](https://github.com/pmcfadin/cqlite/issues/751), [#752](https://github.com/pmcfadin/cqlite/issues/752) |
| BTI (`da`) SSTables are rejected, not read | Use BIG format or convert first | [#660](https://github.com/pmcfadin/cqlite/issues/660) |
| Pre-5.0 formats (`md`/`mc`/`la`/`ma`) unsupported | By design — Cassandra 5.0 only | [Limitations](https://pmcfadin.github.io/cqlite/user-docs/limitations/) |

For what CQLite does **not** do by design (older formats, network access, query
features), see [Limitations](https://pmcfadin.github.io/cqlite/user-docs/limitations/).

**Found something not listed?** [Open an issue](https://github.com/pmcfadin/cqlite/issues/new/choose) — a good report (Cassandra version, schema, command, output) is the most valuable contribution you can make.

## Architecture Highlights

**Design Philosophy:**
- **No cluster dependency** - Read and write SSTables directly, with no running Cassandra node
- **CQL parser** - Native CQL support using an Antlr4 grammar
- **Cassandra 5+ focus** - Modern 'oa' format with BTI support
- **Memory efficient** - <128MB usage target for large files
- **Self-contained engine** - Pure-Rust parsing and writing, including STCS compaction

## Getting Involved

CQLite is developed in the open as an Apache-licensed project. We welcome contributions from the Cassandra community!

### Development Methodology

CQLite uses a **spec-driven, agent-orchestrated, gate-enforced** workflow built on Claude Code. In short:

- **Specs are the source of truth.** Requirements live in a durable [OpenSpec](https://github.com/Fission-AI/OpenSpec) spec under `openspec/specs/`; GitHub issues (epics + sub-issues) are the execution ledger, not the contract. *(spec layer rolling out in the v0.13 cycle.)*
- **A Product-Manager orchestrator** (`/prioritize`, `/pm-status`, `/start-epic`) plans, prioritizes, and coordinates implementer agents — one stream per issue in an isolated git worktree.
- **Every task passes a deterministic gate** (`scripts/agent-gate.sh`: `cargo fmt`, `clippy -D warnings`, tests, smoke) before it's "done" — enforced by a `TaskCompleted` hook, not the honor system.
- **The author is never the reviewer.** Work is reviewed in a fresh context by roborev (a second model family) + `rust-reviewer`, and audited against the spec (`spec-auditor`) and for meaningful coverage (`coverage-reviewer`).
- **Humans decide product, agents decide implementation.** Ambiguous scope and tradeoffs are escalated on a **NEEDS YOU** list, never guessed.

**Definition of done:** gate passes · spec-auditor confirms acceptance criteria · coverage-reviewer confirms tests are meaningful · roborev is clean.

📖 **Full workflow, lifecycle, and how to run it yourself:** [`docs/development/METHODOLOGY.md`](docs/development/METHODOLOGY.md)

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
- **Compression**: LZ4, Snappy, Deflate, Zstd

### Performance Targets
- **Parse Speed**: 1GB files in <10 seconds
- **Memory Usage**: <128MB for large SSTables
- **Query Latency**: Sub-millisecond partition lookups

### Language Bindings
- **Python**: Production-ready sync API (see [Python README](bindings/python/README.md))
- **Node.js**: Production-ready Promise API (see [Node.js README](bindings/node/README.md))
- **WASM**: Planned (M6+)

## Resources

- **Documentation site**: [https://pmcfadin.github.io/cqlite/](https://pmcfadin.github.io/cqlite/) — user docs, SSTable format guide, agent integration docs
- **API docs (rustdoc)**: [latest tag](https://pmcfadin.github.io/cqlite/api/latest/) · published per release tag at `https://pmcfadin.github.io/cqlite/api/<tag>/`
- **Changelog**: [CHANGELOG.md](CHANGELOG.md) — what each tagged release contains
- **Performance**: [Methodology, local repro, and CI gate policy](docs/performance.md)
- **CQL Grammar**: [Patrick's Antlr4 CQL Grammar](https://github.com/pmcfadin/cassandra-antlr4-grammar)
- **Issues**: [GitHub Issues](https://github.com/pmcfadin/cqlite/issues)
- **Discussions**: [GitHub Discussions](https://github.com/pmcfadin/cqlite/discussions)

## Community

- **⭐ Star the project**: [github.com/pmcfadin/cqlite](https://github.com/pmcfadin/cqlite) — the single best way to support it and shape where the time goes
- **🐛 Bugs & feature requests**: [GitHub Issues](https://github.com/pmcfadin/cqlite/issues/new/choose)
- **💬 Questions & ideas**: [GitHub Discussions](https://github.com/pmcfadin/cqlite/discussions)
- **🛠 Contributing**: see [CONTRIBUTING.md](CONTRIBUTING.md), the [Roadmap](#roadmap), and our [Code of Conduct](CODE_OF_CONDUCT.md) — look for `good-first-issue` labels

CQLite is an independent open-source project, not an Apache Software Foundation
project. It is built in the spirit of the Apache Cassandra community, with the
goal of contributing it upstream as it matures.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

Special thanks to the Apache Cassandra community and the many contributors who make projects like this possible. CQLite builds on decades of database engineering innovation from the Cassandra project.

---

**Note**: M1 through M5 milestones are complete and the project is at **v0.12.0**. Core SSTable reading, CLI, output writers (including Parquet), Python and Node.js bindings, and write support with STCS compaction and **byte-for-byte compaction parity vs Apache Cassandra** are production-ready, alongside an Arrow Flight + Trino connector, canonical BTI (`da`) write/read, and CDC-style delta export. Next: M6 (WASM bindings) and M7 (performance validation + v1.0).