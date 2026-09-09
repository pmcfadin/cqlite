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

> **Status**: **v0.17.0** (2026-09-08) — read correctness and read-path performance against **stock Cassandra 5.0**. CQLite reads (and writes) Cassandra 5.0 SSTables with no cluster dependency: CLI, Rust library, Python and Node.js bindings, an Arrow Flight server, and a Trino connector are all shipped and published on every tag. v0.17.0 closed 97 issues: a read-correctness campaign pinned against Cassandra-written bytes (silently truncated rows, a misaligned point-read cell offset, BTI row-index framing, empty collection keys, `inet`/`time` ordering), a measured read-path program (a 3.06× Arrow Flight single-SSTable fast path; 1.25× stock Cassandra's `count(*)` and 1.12× its `SELECT *` on the same core and SSTable), and one value/error contract across Python, Node and the CLI. **[Release notes](docs/releases/RELEASE_NOTES_v0.17.0.md)** · [CHANGELOG.md](CHANGELOG.md) · [all releases](https://pmcfadin.github.io/cqlite/releases/).

> **Upgrading to v0.17?** It carries breaking changes with before/after examples and consumer guidance in the [`[v0.17.0]` CHANGELOG section](CHANGELOG.md): `cqlite-core`'s `Value` gains `Value::Empty`, `Config.storage` is the single storage config and decorative knobs are gone, CLI `--format json` renders `decimal`/`varint` unquoted and drops the injected UDT `_type` key, and the bindings carry UDT type identity out of band. Earlier migrations: [v0.13 Migration Guide](docs/development/v0.13-migration-guide.md).

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
| Releases — what shipped in each version and how to upgrade | [/cqlite/releases/](https://pmcfadin.github.io/cqlite/releases/) |
| Roadmap · Known Issues · Limitations | [/cqlite/user-docs/roadmap/](https://pmcfadin.github.io/cqlite/user-docs/roadmap/) · [/cqlite/user-docs/known-issues/](https://pmcfadin.github.io/cqlite/user-docs/known-issues/) · [/cqlite/user-docs/limitations/](https://pmcfadin.github.io/cqlite/user-docs/limitations/) |
| Field validation — measured runs against live Cassandra clusters | [/cqlite/field-validation/](https://pmcfadin.github.io/cqlite/field-validation/0-16-0-ga/) |
| API docs (rustdoc) | [/cqlite/api/latest/](https://pmcfadin.github.io/cqlite/api/latest/) · [docs.rs/cqlite-core](https://docs.rs/cqlite-core) |

In-repo: [`docs/releases/`](docs/releases/) (per-release notes), [`docs/sstables-definitive-guide/`](docs/sstables-definitive-guide/) (the SSTable format single source of truth), [`docs/reports/`](docs/reports/) (measurement reports), [`docs/development/`](docs/development/) (PRD, methodology, gate ops).

## Vision

CQLite aims to provide a Cassandra-compatible storage engine that shares reconciliation and materialization across analytics, bulk ingestion, and compaction, with incremental integration into Cassandra. The CLI, bindings, and query interfaces remain the accessible product surfaces. See the [Product Requirements](docs/development/PRD.md) for scope, guarantees, and qualification milestones.

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

### ✅ v0.17.0 (Sep 2026) — read correctness + read-path performance vs stock Cassandra 5
- [x] **Read correctness campaign** — row assembly no longer swallows a decode error into a silently truncated row; a P0 misaligned cell offset on point reads is fixed; BTI `Rows.db` row-index root base matches Cassandra's framing; empty map keys / set members are kept, nested types are bounds-checked at every level, `inet`/`time` order as Cassandra orders them; `frozen<scalar>` is refused instead of guessed, which also fixed the writer's `Statistics.db` header type for `frozen<UDT>`
- [x] **Measured read path** — Arrow Flight `do_get` single-SSTable merge bypass (**3.06×** rows/s per core); admission default derived from core count; byte-bounded Arrow batches; box ceiling established at 2.73M rows/s on 6 physical cores; opt-in `jemalloc` feature for `cqlite-flight`
- [x] **One value/error contract across bindings** — shared authoritative error table, 3-way golden parity (same `SELECT` through Python, Node and CLI must agree), UDT identity out of band
- [x] **JSON/CSV and Parquet output parity lanes** against `sstabledump` goldens; platform-hygiene epics (decorative config knobs deleted, dead read metrics wired, dead code removed)
- See [`docs/releases/RELEASE_NOTES_v0.17.0.md`](docs/releases/RELEASE_NOTES_v0.17.0.md)

### ✅ v0.14 → v0.16.1 (Jul 2026) — Flight field-readiness, Trino latency/throughput, CommitLog reader
- [x] **v0.14** Arrow Flight + Trino read path validated against a live at-scale Cassandra deployment; `Index.db` parsed once per open (~100× faster cold index build)
- [x] **v0.15** ~15× warm through-Trino throughput (p50 2.9 s → 227 ms), admission control, saturation gauges, lazy Summary-guided index (O(summary) open), snapshot reuse
- [x] **v0.16** typed collection columns through Trino (`array`/`row`/`map`), weight-balanced split fan-out, `LIMIT`-cancellation hang fixed; **v0.16.1** Cassandra 5.0 **CommitLog segment reader** (library API + `read-commitlog` CLI)
- See [`docs/releases/`](docs/releases/) for each release's notes

### ✅ v0.13.0 (Jul 2026) — the performance release
- [x] **Read-path constant-factor speedups** — query-engine hot-path cleanups (schema `Arc`, single projection, cached sort keys, plan cache), read-path idiom bundle, and point-read I/O via a positional-read (`ReadAt`) trait
- [x] **Node.js bindings throughput** — batch-fetch streaming rows, move (not clone) row values in `executeNative`, cached `Set`/`Map` constructors
- [x] **Byte-bounded result budget** — `Error::ResultTooLarge` + `QueryConfig.max_result_bytes` (default 64 MiB)
- [x] **Per-surface SSTable freshness contract** + explicit `Database.refresh()`
- [x] **No-heuristics correctness** — removed blob-decode byte-pattern guessing; unknown-table reads fail honestly instead of fabricating a default schema
- See [CHANGELOG.md](CHANGELOG.md) for the full per-release detail

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

The [PRD](docs/development/PRD.md#7-milestones-and-dependency-gates) defines the
storage-engine qualification program. M1–M5 remain historical delivery milestones;
the new milestones measure supported workflows and operator outcomes.

| Milestone | Outcome |
|-----------|---------|
| SE1 — Engine contract | Explicit scope, authority, capabilities, and workload acceptance criteria |
| SE2 — Shared offline execution | Safe compaction and reusable analytics materialization |
| SE3 — Analytics offload value | Measured benefit within Cassandra foreground-service and freshness budgets |
| SE4 — Bulk writing and policy tools | Qualified native ingestion plus compaction planning/simulation |
| SE5 — Qualified freshness | Proven memtable-tail behavior for opted-in analytical reads |
| SE6 — Bounded Cassandra integration | One reversible native/offload pilot |
| SE7 — Native lifecycle qualification | Supported integrated behavior backed by operational evidence |

Qualification is pending; existing implementation and research contribute evidence
but do not automatically complete these milestones. External-engine **1.0 requires
SE1–SE4** and the PRD's release gates. WASM and native lifecycle integration are
separate tracks. See the [user roadmap](https://pmcfadin.github.io/cqlite/user-docs/roadmap/).

The roadmap follows real-world use. Want something prioritized?
[Open or 👍 an issue](https://github.com/pmcfadin/cqlite/issues) — and
[⭐ star the repo](https://github.com/pmcfadin/cqlite).

## Known Issues

CQLite is honest about its sharp edges. The current release (`v0.17.0`) has a few
known gaps — none of which block the core read/export workflows. Full, dated list:
[pmcfadin.github.io/cqlite → Known Issues](https://pmcfadin.github.io/cqlite/user-docs/known-issues/).

| Issue | Impact | Tracking |
|-------|--------|----------|
| A scan over an SSTable whose metadata is refused returns empty rows instead of an error | Pre-existing in every release; a malformed file reads as "no data" rather than failing loudly | [#4159](https://github.com/pmcfadin/cqlite/issues/4159) (fix in flight, 0.18) |
| Unbounded `SELECT count(*)` through the Trino `cqlite` catalog can stall | Use a bounded predicate or an aggregate that reads a column; found on a 3-node field run | [#4170](https://github.com/pmcfadin/cqlite/issues/4170) |
| Trino scans are served by the `sidecar-uri` host's Flight pod only | Single-node throughput on multi-node clusters; other replicas idle | [#4175](https://github.com/pmcfadin/cqlite/issues/4175) |
| `timeuuid` maps to `varchar` in the Trino `cqlite` catalog (stock connector: `uuid`) | "Same SQL" does not hold for `timeuuid` columns | [#4173](https://github.com/pmcfadin/cqlite/issues/4173) |
| Cassandra **trunk** SSTables (BIG `pa`, BTI `ea`) are out of scope | BTI `ea` is refused; BIG `pa` needs a version ceiling | [#4142](https://github.com/pmcfadin/cqlite/issues/4142) |
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

- **Specs are the source of truth.** Design-driven work lives in a durable [OpenSpec](https://github.com/Fission-AI/OpenSpec) spec under `openspec/specs/`; oracle-driven bug fixes are a GitHub issue plus a fixture-pinned parity test. GitHub issues and the project board are the execution ledger, not the contract.
- **A delivery pipeline of skills** (`flow-groom` → `flow-activate` → `flow-implement` → `flow-address` → `flow-finalize`, plus `drive-issue` for one issue end to end) runs on a fleet of machines, one issue per worktree/branch/PR, with a server-arbitrated claim ref so two machines cannot take the same issue.
- **Every change passes a deterministic gate** (`scripts/agent-gate.sh`: `cargo fmt`, `clippy -D warnings`, core/integration/write/CLI tests, structural audits, smoke) whose `AGENT-GATE SUMMARY` block is the only run that counts; GitHub branch protection then requires a `required` check that aggregates the CI tiers before any merge.
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
- Tiered test coverage targets (see [historical PRD Section 5.1](docs/development/PRD-toolkit-v0.2.md#51--tiered-coverage-targets))

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

### ✅ v0.10 → v0.17.0 (Jun–Sep 2026)
Eight releases since M5 — compaction parity, Arrow Flight + Trino, BTI write/read, delta export, the performance
release, Flight field-readiness, Trino latency/throughput, the CommitLog reader, and the 0.17 read-correctness +
read-path program. Per-release notes: [`docs/releases/`](docs/releases/) · [website releases](https://pmcfadin.github.io/cqlite/releases/).

See [docs/development/PRD.md](docs/development/PRD.md) for the qualification milestones (SE1–SE7) that follow M1–M5.

## Technical Details

### Supported Formats
- **Cassandra 5.0**: BIG format versions `na`/`nb`/`oa` and BTI (trie-indexed) format `da`; pre-5.0 (`ma`–`me`) is out of scope by design, and trunk's `pa`/`ea` are not supported ([#4142](https://github.com/pmcfadin/cqlite/issues/4142))
- **SSTable components**: Data.db, Index.db, Summary.db, Statistics.db, CompressionInfo.db, Filter.db, TOC.txt, Digest.crc32; BTI Partitions.db / Rows.db
- **CommitLog**: Cassandra 5.0 segment files (version 7), read-only (`read-commitlog`)
- **Compression**: LZ4, Snappy, Deflate, Zstd (reads); the write surface emits uncompressed SSTables only
- **Format authority**: the pinned `cassandra-5.0.8` source and `sstabledump` output — never CQLite's own prior behaviour ([SSTable definitive guide](docs/sstables-definitive-guide/README.md))

### Performance (measured, not targeted)
- **Bare scan ceiling**: 2.73M rows/s on 6 physical cores at 93.5% marginal efficiency ([#3299](https://github.com/pmcfadin/cqlite/issues/3299))
- **Same core, same SSTable vs stock Cassandra**: 1.25× its `count(*)` on the read path, 1.12× its `SELECT *` when shipping every row over Arrow Flight; ~5× less memory per row ([`docs/reports/ws0-3100-report.md`](docs/reports/ws0-3100-report.md))
- **3-node Cassandra 5.0.9 cluster, through Trino**: a full-table analytic costs the OLTP client 2.21× baseline p99 via CQLite vs 5.61× via Cassandra's own CQL path; same SQL 1.66× faster on a 22M-row key-value table; 1.72M rows/s sustained from one Flight pod ([`docs/2024-09-meetup/REPORT.md`](docs/2024-09-meetup/REPORT.md), single-pod — see [#4175](https://github.com/pmcfadin/cqlite/issues/4175))
- **Memory**: <128 MB target for the core library on large SSTables; Flight server RSS under load is an open measurement
- Method and CI gate policy: [`docs/performance.md`](docs/performance.md)

### Language Bindings
- **Python**: Production-ready sync API (see [Python README](bindings/python/README.md))
- **Node.js**: Production-ready Promise API (see [Node.js README](bindings/node/README.md))
- **WASM**: Deferred optional surface; not a prerequisite for external-engine 1.0

## Resources

- **Documentation site**: [https://pmcfadin.github.io/cqlite/](https://pmcfadin.github.io/cqlite/) — user docs, SSTable format guide, agent integration docs
- **API docs (rustdoc)**: [latest tag](https://pmcfadin.github.io/cqlite/api/latest/) · published per release tag at `https://pmcfadin.github.io/cqlite/api/<tag>/`
- **Releases**: [`docs/releases/`](docs/releases/) (per-release notes) · [website releases page](https://pmcfadin.github.io/cqlite/releases/) · [GitHub releases](https://github.com/pmcfadin/cqlite/releases) (binaries, wheels, checksums)
- **Changelog**: [CHANGELOG.md](CHANGELOG.md) — what each tagged release contains, with every breaking change
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

**Planning note**: M1–M5 record the toolkit and execution foundation. The [current PRD](docs/development/PRD.md) replaces the remaining M6/M7 sequence with SE1–SE7 qualification milestones. Release-specific support and parity claims must follow the [parity release checklist](docs/development/parity-release-checklist.md).
