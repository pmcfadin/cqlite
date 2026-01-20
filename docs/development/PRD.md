# CQLite – Concise Product Requirements Document (v0.2)

## 1 · Mission & Vision

* **Mission**  Lower friction to Cassandra data by providing a fast, safe Rust library for local SSTable operations—fully aligned with Apache community values.
* **Vision**  Become the de‑facto community standard for reading Cassandra 5+ SSTables, usable from CLI, Python, Node.js, and WASM.

---

## 2 · Functional Scope (Must‑Have for v1.0)

| Area                    | Requirements                                                                                                                                                                                                                                     |     |                                                                    |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --- | ------------------------------------------------------------------ |
| **Core Reading**        | • 100 % Cassandra 5 SSTable format support (data, TOC, index, stats)<br>• All CQL types incl. collections & UDTs<br>• Compression: LZ4, Snappy, Deflate<br>• Zero‑copy deserialization into **provided schema** (schema passed in, not inferred) |     |                                                                    |
| **CLI (`cqlite`)**      | • **One‑shot mode**: `--schema`, `--data-dir`, optional `--query` & \`--out {json                                                                                                                                                                | csv | parquet}\`<br>• **REPL mode**: interactive attach / query / export |
| **Output Formats**      | JSON, CSV, Parquet (pluggable writers)                                                                                                                                                                                                           |     |                                                                    |
| **Language Bindings**   | Typed APIs for Python (sync), Node.js (TS defs); WASM deferred to M6                                                                                                                                                                               |     |                                                                    |
| **Writing**             | Generate Cassandra 5 SSTables (M5)                                                                                                                                                                                                                     |     |                                                                    |
| **Performance Targets** | Set after functional parity; goal: *faster than native Cassandra bulk tools*                                                                                                                                                                     |     |                                                                    |

---

## 3 · Architecture & Separation of Concerns

```
cqlite-core/        # Pure Rust crate
├── sstable_rw/     # read/write, compression, checksums
├── schema/         # type system, validation
└── query/          # minimal SQL parser & executor (optional)

cli/                # REPL + one-shot wrapper (uses core)
bindings/
  ├── python/
  ├── node/
  └── wasm/
tests/              # shared fixtures (Cassandra 5 SSTables)
```

* **Rule**: Core never depends on CLI or bindings.
* **Async‑first**, **type‑safe**, zero‑copy IO.

---

## 4 · Milestones

| #      | Deliverable                | Key Exit Criteria                                                                         |
| ------ | -------------------------- | ----------------------------------------------------------------------------------------- |
| **M1** | **Core Reading Library**   | Reads any Cassandra 5 SSTable; all CQL/UDT types; compression OK; tiered coverage (see Section 5.1) |
| **M2** | **CLI (REPL + one‑shot)**  | Human can query & verify data from disk; basic `SELECT … WHERE …`                         |
| **M3** | **Output Writers**         | JSON, CSV, Parquet export work end‑to‑end via CLI                                         |
| **M4** | **Python & Node.js Bindings** | `pip install cqlite`, `npm i @cqlite/node`; CI wheels & native modules                 |
| **M5** | **Write Support**          | Generate valid Cassandra 5 SSTables; write API in core and bindings                       |
| **M6** | **WASM Bindings**          | `npm i @cqlite/wasm`; IndexedDB support; browser compatibility                            |
| **M7** | **Perf & Size Validation** | Benchmarks > native bulk tools; WASM < 2 MB; publish v1.0 release                         |

> **Revision Note (Jan 2026)**: M1 coverage revised to tiered targets per Issue #204. M4 now Python & Node.js only (sync Python first). Write Support restored as M5. WASM moved to M6. v1.0 release moved to M7.


---

## 5 · Testing Strategy

| Layer       | Tests                                              | Tooling                            |
| ----------- | -------------------------------------------------- | ---------------------------------- |
| Core        | Unit + property‑based for type/format edge cases   | Rust `cargo test`, `proptest`      |
| CLI         | Integration & snapshot tests for commands/output   | `assert_cmd`, `insta`              |
| Bindings    | Language‑specific unit + FFI smoke tests           | `pytest`, `jest`, web‑worker tests |
| Integration | End‑to‑end: read → export → read‑back              | GitHub Actions matrix              |
| CI/CD       | PR lint, fmt, unit, integration; codecov gate 75 % | GitHub Actions                     |

### 5.1 · Tiered Coverage Targets

Coverage targets are tiered by module criticality rather than flat percentages:

| Tier | Line Coverage | Branch Coverage | Modules |
|------|---------------|-----------------|---------|
| **Critical** | 90%+ | 80%+ | `parser/`, `storage/sstable/reader/`, `storage/sstable/reader/parsing/` |
| **Important** | 80%+ | 70%+ | `query/`, `schema/`, `types/`, `cql/`, `discovery/` |
| **Supporting** | 70%+ | 60%+ | `memory/`, `platform/`, `storage/sstable/directory/`, `storage/sstable/bti/` |
| **Utilities** | 50%+ | 40%+ | `benchmarks/`, `testing/` |

**Aggregate Target**: 75% overall (weighted by module size), enforced via codecov gate.

**Rationale**: Tiered coverage focuses testing effort on critical code paths (parser, storage) where bugs cause data corruption, while allowing pragmatic coverage for utilities and platform abstractions.

---

## 6 · Release & Distribution

* **CI**: GitHub Actions builds artifacts per tag.
* **Packages**:

  * Cargo crate (`cqlite-core`)
  * PyPI wheels (`cqlite`) for macOS/Linux/Win
  * npm (`@cqlite/node`) native pre-builds; WASM (`@cqlite/wasm`) in M6
  * Homebrew & Linuxbrew taps for CLI
* **Versioning**: SemVer; v0.x during feature development, v1.0 at M6 completion.

### 6.1 · Python Release Process

**Workflows**:
- `.github/workflows/python-ci.yml` - Build and test on push/PR
- `.github/workflows/python-release.yml` - Publish to PyPI on tags

**Triggering a Release**:
1. Update version in `bindings/python/pyproject.toml`
2. Create and push a version tag:
   - **Stable release**: `git tag v0.3.0 && git push origin v0.3.0` → Publishes to PyPI
   - **Pre-release**: `git tag v0.3.0-rc1 && git push origin v0.3.0-rc1` → Publishes to TestPyPI

**Tag Format**:
| Pattern | Example | Destination |
|---------|---------|-------------|
| `v*` (no suffix) | `v0.3.0`, `v1.0.0` | PyPI (production) |
| `v*-rc*` | `v0.3.0-rc1` | TestPyPI |
| `v*-alpha*` | `v0.3.0-alpha1` | TestPyPI |
| `v*-beta*` | `v0.3.0-beta1` | TestPyPI |

**Authentication**: Uses PyPI Trusted Publishing (OIDC) - no API tokens stored in secrets.

**Platform Matrix** (5 wheels built):
- Linux x86_64 (manylinux_2_28)
- Linux ARM64 (manylinux_2_28)
- macOS x86_64
- macOS ARM64 (Apple Silicon)
- Windows x64

**PyPI Setup** (one-time, by repo owner):
1. Create PyPI project at https://pypi.org/manage/projects/
2. Add trusted publisher at Settings → Publishing:
   - Owner: `pmcfadin`
   - Repository: `cqlite`
   - Workflow: `python-release.yml`
3. Repeat for TestPyPI at https://test.pypi.org/

---

## 6.2 · Python API Reference

The Python bindings provide a synchronous API for reading Cassandra 5.0 SSTables.

### Installation

```bash
pip install cqlite
```

### Quick Start

```python
import cqlite

# Open database with schema
with cqlite.open("path/to/sstables", schema="schema.cql") as db:
    # Execute query
    result = db.execute("SELECT * FROM keyspace.table LIMIT 10")

    # Iterate over rows
    for row in result:
        print(row["column_name"])
```

### Core Classes

| Class | Description |
|-------|-------------|
| `Database` | Main entry point for querying SSTables |
| `QueryResult` | Contains all rows from a query execution |
| `Row` | Dict-like access to column values |
| `StreamingIterator` | Memory-efficient iteration for large datasets |
| `PreparedStatement` | Query plan analysis |
| `DatabaseStats` | Storage, memory, and query metrics |
| `StreamingConfig` | Configuration for streaming queries |

### Exception Hierarchy

| Exception | When Raised |
|-----------|-------------|
| `CqliteError` | Base for all CQLite errors |
| `SchemaError` | Schema parsing or validation fails |
| `QueryError` | Query execution fails |
| `ParseError` | CQL syntax error |

### CQL Type Mappings

| CQL Type | Python Type | Notes |
|----------|-------------|-------|
| `boolean` | `bool` | |
| `int`, `bigint`, `varint` | `int` | Arbitrary precision |
| `float`, `double` | `float` | |
| `decimal` | `decimal.Decimal` | Precision preserved |
| `text`, `ascii` | `str` | |
| `blob` | `bytes` | |
| `timestamp` | `datetime.datetime` | UTC timezone-aware |
| `date` | `datetime.date` | |
| `time` | `datetime.time` | Microsecond precision |
| `duration` | `datetime.timedelta` | Month ≈ 30 days |
| `uuid`, `timeuuid` | `uuid.UUID` | |
| `inet` | `ipaddress.IPv4Address` / `IPv6Address` | |
| `list<T>` | `list` | |
| `set<T>` | `frozenset` | Hashable |
| `map<K,V>` | `dict` | |
| `tuple` | `tuple` | |
| UDT | `dict` | With `_type`, `_keyspace` fields |

### Type Checking

CQLite ships with PEP 561 type stubs for full mypy/pyright support:

```python
# mypy will validate these types
import cqlite

db: cqlite.Database = cqlite.open("data")
result: cqlite.QueryResult = db.execute("SELECT * FROM ks.tbl")
row: cqlite.Row = result.rows[0]
value: str = row["name"]  # Type narrowing works
```

### Streaming for Large Datasets

```python
# Memory-efficient streaming (< 128 MB for any size)
config = cqlite.StreamingConfig(buffer_size=512, chunk_size=5000)
for row in db.execute_streaming("SELECT * FROM big_table", config=config):
    process(row)
```

### Thread Safety

- Database handle is thread-safe via `Arc<Database>`
- `close()` is idempotent and safe from any thread
- Each thread should use its own `StreamingIterator`
- Known: Concurrent queries may need warm-up query first (Issue #311)

---

## 7 · Community & Governance (Snapshot)

* Apache 2.0 license from day 1; CLA + DCO required.
* Public GitHub project board, weekly community call.
* Donation path: engage Cassandra PMC by M4, IP clearance by M6.

---

## 8 · Risks & Mitigations (Top 3)

| Risk                   | Impact                       | Mitigation                                            |
| ---------------------- | ---------------------------- | ----------------------------------------------------- |
| Cassandra format churn | Read/write breakage          | Modular format adapters + test corpus per release     |
| WASM memory limits     | Feature gaps in browser env. | IndexedDB chunked IO + streaming deserialization      |
| External PR quality    | Project instability          | Strict CI gates, contributor guide, mandatory reviews |

---

## 9 · Acceptance / “Definition of Done”

1. **Functional parity** – read SSTables for all Cassandra 5+ formats, CLI & bindings pass all tests.
2. **Performance** – demonstrably faster bulk reads than Cassandra native tools.
3. **Coverage quality** – tiered targets met (Critical: 90%+, Important: 80%+, Supporting: 70%+).
4. **Size** – WASM bundle ≤ 2 MB compressed.
5. **Community** – ≥ 10 active contributors, docs & governance ready for ASF.
6. **Release** – v1.0 tagged, packages in Cargo, PyPI, npm; announcement blog post.
