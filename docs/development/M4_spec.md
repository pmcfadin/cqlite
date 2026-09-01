# M4 Language Bindings Specification

**Version**: 1.1
**Status**: Approved
**Created**: January 2026
**Updated**: January 2026
**Milestone**: M4 (Python & Node.js Bindings)

## Executive Summary

M4 delivers language bindings for Python and Node.js, enabling developers to read Cassandra 5.0 SSTables from their preferred language. WASM bindings are deferred to M6 to reduce complexity and ship production bindings sooner.

**Deliverables**:
- `bindings/python/` - Python bindings via PyO3 (`pip install cqlite-py`)
- `bindings/node/` - Node.js bindings via napi-rs (`npm i @cqlite/node`)

**Deferred to M6**:
- `bindings/wasm/` - WebAssembly bindings (`npm i @cqlite/wasm`)

**Exit Criteria**:
- All bindings pass integration tests with real SSTable data
- CI/CD publishes to PyPI and npm on tagged releases
- TypeScript definitions for Node.js
- Documentation and examples for each language

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Shared Design Principles](#2-shared-design-principles)
3. [Python Bindings](#3-python-bindings)
4. [Node.js Bindings](#4-nodejs-bindings)
5. [Type Mapping Reference](#5-type-mapping-reference)
6. [CI/CD Infrastructure](#6-cicd-infrastructure)
7. [Testing Strategy](#7-testing-strategy)
8. [Implementation Phases](#8-implementation-phases)
9. [Risks and Mitigations](#9-risks-and-mitigations)
10. [WASM Deferral Notes](#10-wasm-deferral-notes)

---

## 1. Architecture Overview

### 1.1 Dependency Graph

```
                    ┌─────────────────┐
                    │   cqlite-core   │
                    │  (Rust library) │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
     ┌───────────────┐             ┌───────────────┐
     │bindings/python│             │ bindings/node │
     │    (PyO3)     │             │  (napi-rs)    │
     └───────┬───────┘             └───────┬───────┘
             │                             │
             ▼                             ▼
        pip install                   npm install
          cqlite                     @cqlite/node
```

### 1.2 Core API Surface to Expose

From `cqlite-core/src/lib.rs`, the key entry points are:

| Method | Description | Binding Priority |
|--------|-------------|------------------|
| `Database::open()` | Open database with config | P0 (Required) |
| `Database::execute()` | Execute CQL query | P0 (Required) |
| `Database::execute_streaming()` | Memory-efficient streaming | P0 (Required) |
| `Database::prepare()` | Prepare statement | P1 (Important) |
| `Database::explain()` | Query plan explanation | P2 (Nice-to-have) |
| `Database::stats()` | Database statistics | P1 (Important) |
| `Database::close()` | Cleanup resources | P0 (Required) |

### 1.3 Workspace Structure

```
cqlite/
├── cqlite-core/          # Existing core library
├── cqlite-cli/           # Existing CLI
├── bindings/             # NEW: Language bindings
│   ├── python/           # Python bindings (PyO3)
│   │   ├── Cargo.toml
│   │   ├── pyproject.toml
│   │   ├── src/
│   │   └── python/cqlite/
│   └── node/             # Node.js bindings (napi-rs)
│       ├── Cargo.toml
│       ├── package.json
│       ├── src/
│       └── npm/          # Platform packages
└── tests/                # Shared fixtures
```

---

## 2. Shared Design Principles

### 2.1 Async Runtime Management

Both bindings manage the tokio runtime internally:

```rust
// Pattern: Global tokio runtime per binding
static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

fn get_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime")
    })
}
```

### 2.2 Memory Constraints

- **Target**: <128MB peak memory usage
- **Streaming required**: Use `execute_streaming()` for large result sets
- **Buffer configuration**: Expose `StreamingConfig` to control memory

### 2.3 Error Handling

Map `cqlite_core::Error` to language-native exceptions:

| Rust Error | Python | Node.js/TS |
|------------|--------|------------|
| `Error::Io` | `IOError` | `Error(code: 'IO')` |
| `Error::Schema` | `SchemaError` | `Error(code: 'SCHEMA')` |
| `Error::QueryExecution` | `QueryError` | `Error(code: 'QUERY')` |
| `Error::CqlParse` | `ParseError` | `Error(code: 'PARSE')` |

### 2.4 Configuration

All bindings accept configuration via:
1. Language-native options object/dict
2. JSON string (parsed to `cqlite_core::Config`)
3. Convenience presets (`memory_optimized`, `performance_optimized`)

### 2.5 Streaming Configuration

Expose `StreamingConfig` for memory-constrained execution:

```python
# Python
for row in db.execute_streaming(
    "SELECT * FROM large_table",
    buffer_size=1024,
    chunk_size=100
):
    process(row)
```

```typescript
// Node.js
for await (const row of db.executeStreaming(query, {
  bufferSize: 1024,
  chunkSize: 100,
})) {
  console.log(row);
}
```

---

## 3. Python Bindings

### 3.1 Technology Stack

| Component | Choice | Version |
|-----------|--------|---------|
| FFI | PyO3 | 0.23+ |
| Build System | Maturin | 1.7+ |
| Python Support | Python 3.9+ | ABI3 |

### 3.2 API Design (Sync-First)

Python bindings ship with synchronous API first. Async support will be added in a future release.

```python
# Synchronous API (Primary - M4)
import cqlite

db = cqlite.open("/path/to/data", schema="/path/to/schema.cql")
result = db.execute("SELECT * FROM users LIMIT 10")
for row in result:
    print(row["name"])
db.close()

# Context manager
with cqlite.open("/path/to/data") as db:
    for row in db.execute_streaming("SELECT * FROM large_table"):
        process(row)

# Streaming with explicit config
with cqlite.open("/path/to/data") as db:
    config = cqlite.StreamingConfig(buffer_size=1024, chunk_size=100)
    for row in db.execute_streaming("SELECT * FROM users", config=config):
        print(row)
```

**Async API (Deferred)**:
```python
# Future release - async module
import cqlite.aio

async def main():
    async with cqlite.aio.open("/path/to/data") as db:
        async for row in db.execute_streaming("SELECT * FROM users"):
            print(row)
```

### 3.3 Type Stubs

```python
# cqlite/__init__.pyi
from pathlib import Path
from typing import Iterator, Any, Optional

# Config can be a dict or JSON string for flexibility
Config = dict[str, Any] | str

class StreamingConfig:
    """Configuration for streaming query execution. Values are row counts."""
    def __init__(
        self,
        buffer_size: int = 1024,   # rows in flight (matches core default)
        chunk_size: int = 10_000,  # rows per chunk (matches core default)
    ) -> None: ...

class Database:
    @staticmethod
    def open(
        path: str | Path,
        *,
        schema: str | Path | None = None,
        config: Config | None = None,
    ) -> Database: ...

    def execute(self, query: str) -> QueryResult: ...
    def execute_streaming(
        self,
        query: str,
        config: StreamingConfig | None = None,
    ) -> Iterator[Row]: ...
    def prepare(self, query: str) -> PreparedStatement: ...
    def stats(self) -> DatabaseStats: ...
    def close(self) -> None: ...
    def __enter__(self) -> Database: ...
    def __exit__(self, *args) -> None: ...

class QueryResult:
    rows: list[Row]
    rows_affected: int
    execution_time_ms: int
    columns: list[ColumnInfo]

    def __iter__(self) -> Iterator[Row]: ...
    def to_dict(self) -> dict: ...

class Row:
    def __getitem__(self, key: str) -> Any: ...
    def keys(self) -> list[str]: ...
    def to_dict(self) -> dict[str, Any]: ...
```

### 3.4 Crate Structure

```
bindings/python/
├── Cargo.toml
├── pyproject.toml
├── src/
│   ├── lib.rs              # Module definition
│   ├── database.rs         # PyDatabase wrapper
│   ├── config.rs           # Configuration + StreamingConfig
│   ├── result.rs           # QueryResult, Row, Iterator
│   ├── value.rs            # Value → Python conversion
│   └── error.rs            # Error mapping
├── python/
│   └── cqlite/
│       ├── __init__.py
│       └── __init__.pyi    # Type stubs
└── tests/
    ├── test_basic.py
    ├── test_streaming.py
    └── test_types.py
```

### 3.5 Cargo.toml

```toml
[package]
name = "cqlite-py"
version.workspace = true
edition.workspace = true

[lib]
name = "cqlite"
crate-type = ["cdylib"]

[dependencies]
cqlite-core = { path = "../../cqlite-core", features = ["state_machine", "all-compression"] }
pyo3 = { version = "0.23", features = ["extension-module", "abi3-py39"] }
tokio = { workspace = true, features = ["rt-multi-thread", "sync"] }
serde_json = { workspace = true }
thiserror = { workspace = true }
```

### 3.6 CI/CD for Python

```yaml
# Multi-platform wheel builds
strategy:
  matrix:
    include:
      - os: ubuntu-latest
        target: x86_64-unknown-linux-gnu
        manylinux: "2_28"
      - os: ubuntu-latest
        target: aarch64-unknown-linux-gnu
        manylinux: "2_28"
      - os: macos-13
        target: x86_64-apple-darwin
      - os: macos-14
        target: aarch64-apple-darwin
      - os: windows-latest
        target: x86_64-pc-windows-msvc

steps:
  - uses: PyO3/maturin-action@v1
    with:
      target: ${{ matrix.target }}
      args: --release --out dist
```

---

## 4. Node.js Bindings

### 4.1 Technology Stack

| Component | Choice | Version |
|-----------|--------|---------|
| FFI | napi-rs | 2.16+ |
| Build System | @napi-rs/cli | 2.18+ |
| Node.js Support | N-API v9 | Node 18+ |
| TypeScript | Auto-generated | 5.x |

### 4.2 API Design

```typescript
import { Database, StreamingConfig } from '@cqlite/node';

// Promise-based API
const db = await Database.open('/path/to/data', {
  schema: '/path/to/schema.cql',
  memoryLimit: 128 * 1024 * 1024,
});

const result = await db.execute('SELECT * FROM users LIMIT 10');
console.log(result.rows);

// Streaming with async iterator
for await (const row of db.executeStreaming('SELECT * FROM large_table')) {
  console.log(row);
}

// Streaming with explicit config
const config: StreamingConfig = { bufferSize: 1024, chunkSize: 100 };
for await (const row of db.executeStreaming('SELECT * FROM large_table', config)) {
  console.log(row);
}

await db.close();
```

### 4.3 TypeScript Definitions

```typescript
// Auto-generated with manual refinements
export interface DatabaseOptions {
  schema?: string;
  memoryLimit?: number;
  cacheEnabled?: boolean;
}

export interface StreamingConfig {
  bufferSize?: number;
  chunkSize?: number;
}

export class Database {
  static open(dataDir: string, options?: DatabaseOptions): Promise<Database>;
  execute(query: string): Promise<QueryResult>;
  executeStreaming(query: string, config?: StreamingConfig): AsyncIterable<Row>;
  prepare(query: string): Promise<PreparedQuery>;
  getStats(): Promise<DatabaseStats>;
  close(): Promise<void>;
}

export interface QueryResult {
  rows: Row[];
  rowsAffected: number;
  columns: ColumnInfo[];
  executionTimeMs: number;
}

export interface Row {
  [column: string]: Value;
}

export type Value = null | boolean | number | bigint | string | Buffer | Date | Value[] | Map<Value, Value>;
```

### 4.4 Package Structure

```
bindings/node/
├── Cargo.toml
├── package.json
├── npm/                    # Platform-specific packages
│   ├── darwin-arm64/
│   ├── darwin-x64/
│   ├── linux-arm64-gnu/
│   ├── linux-x64-gnu/
│   └── win32-x64-msvc/
├── src/
│   ├── lib.rs              # Entry point
│   ├── database.rs         # Database wrapper
│   ├── query_result.rs     # Result types
│   ├── streaming.rs        # AsyncIterator
│   ├── types.rs            # Value conversion
│   └── error.rs            # Error handling
├── index.js                # Platform loader
├── index.d.ts              # TypeScript definitions
└── __test__/
    ├── database.spec.ts
    └── streaming.spec.ts
```

### 4.5 Cargo.toml

```toml
[package]
name = "cqlite-node"
version.workspace = true
edition.workspace = true

[lib]
crate-type = ["cdylib"]

[dependencies]
cqlite-core = { path = "../../cqlite-core", features = ["state_machine", "all-compression"] }
napi = { version = "2.16", features = ["async", "napi9", "tokio_rt", "serde-json"] }
napi-derive = "2.16"
tokio = { workspace = true, features = ["rt-multi-thread", "sync"] }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }

[build-dependencies]
napi-build = "2.1"
```

### 4.6 package.json

```json
{
  "name": "@cqlite/node",
  "version": "0.3.0",
  "main": "index.js",
  "types": "index.d.ts",
  "napi": {
    "name": "cqlite-node",
    "triples": {
      "defaults": true,
      "additional": ["aarch64-apple-darwin", "aarch64-unknown-linux-gnu"]
    }
  },
  "scripts": {
    "build": "napi build --platform --release",
    "test": "jest"
  },
  "devDependencies": {
    "@napi-rs/cli": "^2.18.0",
    "jest": "^29.0.0",
    "@types/jest": "^29.0.0",
    "typescript": "^5.0.0"
  }
}
```

**Critical**: The `napi.name` field must be `"cqlite-node"` to correctly resolve the binary from the scoped package `@cqlite/node`.

---

## 5. Type Mapping Reference

### 5.1 CQL to Language Type Mapping

| CQL Type | Rust (cqlite-core) | Python | Node.js/TS |
|----------|-------------------|--------|------------|
| `boolean` | `bool` | `bool` | `boolean` |
| `tinyint` | `i8` | `int` | `number` |
| `smallint` | `i16` | `int` | `number` |
| `int` | `i32` | `int` | `number` |
| `bigint` | `i64` | `int` | `bigint` |
| `counter` | `i64` | `int` | `bigint` |
| `float` | `f32` | `float` | `number` |
| `double` | `f64` | `float` | `number` |
| `decimal` | `Decimal` | `decimal.Decimal` | `string` |
| `varint` | `Vec<u8>` | `int` | `bigint` |
| `text` | `String` | `str` | `string` |
| `ascii` | `String` | `str` | `string` |
| `blob` | `Vec<u8>` | `bytes` | `Buffer` |
| `timestamp` | `i64` | `datetime.datetime` | `Date` |
| `date` | `i32` | `datetime.date` | `Date` |
| `time` | `i64` | `datetime.time` | `bigint` (ns) |
| `uuid` | `[u8; 16]` | `uuid.UUID` | `string` |
| `timeuuid` | `[u8; 16]` | `uuid.UUID` | `string` |
| `inet` | `Vec<u8>` | `ipaddress` | `string` |
| `duration` | `Duration` | `timedelta` | `object` |
| `list<T>` | `Vec<Value>` | `list` | `T[]` |
| `set<T>` | `Vec<Value>` | `frozenset` | `Set<T>` |
| `map<K,V>` | `Vec<(K,V)>` | `dict` | `Map<K,V>` |
| `tuple<...>` | `Vec<Value>` | `tuple` | `[...]` |
| `udt` | `UdtValue` | `dict` | `object` |
| `null` | `Value::Null` | `None` | `null` |

> **Note:** superseded in v0.13 — the Python `time`/`duration` mappings above are the pre-0.13 (lossy) types. As of v0.13, `time`→`int` (ns since midnight) and `duration`→`cqlite.Duration(months, days, nanos)`. See the [v0.13 Migration Guide](./v0.13-migration-guide.md).

### 5.2 Implementation Notes

**Python Collection Behavior (Issue #301)**:

1. **Map Key Hashability**: CQL allows collection types (`list<int>`, `set<text>`) as map keys. Python `dict` requires keys to be hashable (mutable lists are not). When converting CQL `map<K,V>` to Python:
   - If K is `list<T>` → convert to `tuple` before using as dict key
   - If K is `set<T>` → convert to `frozenset` before using as dict key
   - Failure to handle this will cause `TypeError: unhashable type` at runtime

2. **Set Elements**: Sets also use hashable conversion internally, ensuring nested collections within sets are hashable (`set<frozen<list<int>>>` works correctly)

3. **UDT type identity is carried OUT OF BAND (issue #3504)**: a CQL UDT converts to a
   `cqlite.Udt`, not a `dict`:
   - `.type_name`: the UDT type name (e.g. `"address_type"`)
   - `.keyspace`: the keyspace containing the UDT definition
   - `.fields`: the declared fields, name → value, and **nothing else**
   - all UDT fields remain accessible by name via the mapping protocol (`udt["street"]`,
     `"city" in udt`, `len(udt)`, `iter(udt)`, `keys`/`values`/`items`), all delegating to `.fields`
   - null UDT fields return Python `None`
   - **Breaking change from the pre-#3504 shape.** `_type`/`_keyspace` used to be *injected as dict
     keys*, i.e. into the same namespace as the user-controlled field names, so a UDT declaring a
     field of either name overwrote the marker. Read the identity as `.type_name`/`.keyspace`;
     `udt["_type"]` now reaches a FIELD of that name and raises `KeyError` when none is declared.

4. **Frozen Collection Unwrapping**: `FROZEN<T>` collections are transparently unwrapped to their inner type. `FROZEN<list<int>>` returns a Python `list`, not a special frozen type.

5. **Nested Collections**: All nested structures are recursively converted:
   - `MAP<TEXT, FROZEN<LIST<INT>>>` → `dict[str, list[int]]`
   - `MAP<TEXT, FROZEN<SET<TEXT>>>` → `dict[str, frozenset[str]]`
   - `LIST<FROZEN<udt>>` → `list[dict]` with UDT metadata

### 5.3 Collection Identity Semantics: Python ↔ Node (Issue #1454)

The two bindings represent CQL collections with **different host types and different
identity rules**. This section states those semantics explicitly so that anything
comparing the bindings — in particular the 3-way golden parity harness (**#1455**, Y1) —
canonicalizes by contract rather than by accident.

**Authority.** Every row below was read from source (re-verified 2026-08-29). Python:
`bindings/python/src/value.rs` — `list_to_py`, `set_to_py`, `map_to_py`, `tuple_to_py`; and
`bindings/python/src/value_hashable.rs` — `value_to_hashable_key`, `json_to_hashable_key` and
`set_to_py`'s `contains_udt` helper, split out of `value.rs` by #3500. Node:
`bindings/node/src/value.rs` — `list_to_array`, `set_to_js_set`, `map_to_js_map`, and the
`Value::Tuple(items) => list_to_array(ctx, items)` arm of `value_to_napi`. Function names
are cited instead of line numbers because line numbers drift.

| CQL type | Python host type | Node host type | Identity semantics | Asymmetry |
|---|---|---|---|---|
| `list<T>` | `list` (`list_to_py`) | `Array` (`list_to_array`) | positional; order preserved on both sides; no dedupe | **symmetric** |
| `set<scalar>` | `frozenset` (`set_to_py`, non-UDT branch; elements go through `value_to_hashable_key`) | `Set` (`set_to_js_set`, `new Set(array)`) | Python: hash/`__eq__` value-equality. Node: SameValueZero — for scalars this is also value-equality | container type differs; **element identity agrees** for scalars. Iteration order differs: `frozenset` is hash-ordered, JS `Set` is insertion-ordered — canonicalize by sorting |
| `set<frozen<udt>>` | `list` — **fallback**, now a deliberate CLI-parity choice rather than a hard impossibility: since #3504 a UDT is a HASHABLE `cqlite.Udt`, so a `frozenset` would be possible; `set_to_py` still takes this branch when `contains_udt` is true for any element, matching the CLI's array rendering (#804), and changing it would change the observable shape of every such column. `contains_udt` is a FULL subtree traversal since #3500, so the branch is taken for a UDT at ANY depth — under a `tuple`, a nested `set`, a `list`, or either side of a `map` — and not only directly under `frozen`, which makes the shape uniform instead of nesting-dependent | `Set` of objects (`set_to_js_set`; no UDT fallback exists on the Node side) | Python: none — a `list` does not dedupe. Node: SameValueZero on **objects = reference identity**, so structurally-equal UDT elements are *not* deduped either | **asymmetric container**: Python degrades to `list`, Node keeps `Set`. Both are effectively order-preserving and non-deduping, so a set-of-UDT round-trips as a sequence on both sides. **Consequence for canonicalization:** because the Python side is a plain `list`, it cannot be sorted without also reordering genuine `list<T>` values, so this row's canonical form is **order-sensitive** — two structurally-equal UDT sets in different orders compare unequal (instance **b-1** in the canonicalization section below) |
| `map<k,v>` | `dict` (`map_to_py`); keys are the **hashable projection** `value_to_hashable_key` (TOTAL and exhaustive over `Value` since #3500 — `list`/`tuple`→`tuple`, `set`→`frozenset`, `map`→tuple of pairs, `udt`→a `cqlite.Udt` instance carrying identity out of band with each field value recursively projected (#3504; it used to flatten to a `frozenset` of `(name, value)` pairs incl. `_type`/`_keyspace`), `json`→`tuple`/`frozenset`, `frozen`→recurse, every scalar→`value_to_py`; there is no `_ =>` arm and no fall-through, pinned by `#[deny(clippy::wildcard_enum_match_arm)]`), values are the ordinary `value_to_py`. **Superseded measurement, kept because it dates the two fixes (#3504 R1-2, fixture table `test_udt_collision.udt_hashable_shapes`):** while `set` and `tuple` still had no arm and fell through to `value_to_py`, a UDT nested in a **`tuple`** projected only incidentally — the fallthrough happened to yield a hashable `tuple` of `cqlite.Udt`, where before #3504 it was a `dict` and the read raised `TypeError: unhashable type: 'dict'` — while a UDT nested in a **`set`** still raised `TypeError: unhashable type: 'list'`, because `set_to_py` renders a UDT-bearing set as a `list` for CLI parity. #3500 gave both variants their own arms, so neither outcome depends on a fallthrough any more | `Map` (`map_to_js_map`, `new Map(entries)`); **both** key and value use the ordinary `value_to_napi` | Python: keys collapse by hash/`__eq__` — writing an equal key overwrites, last-value-wins. Node: keys collapse by SameValueZero, so scalar keys collapse but **object keys (UDT / list / tuple keys) are compared by reference and never collapse** | **two asymmetries.** (1) *dict-key collapse*: structurally-equal non-scalar keys collapse in Python and survive as distinct entries on Node. (2) *key shape*: a Python map **key** is a hashable projection, which for non-UDT non-scalars still changes the host shape (a `map` key becomes a tuple of pairs, a `list` or `tuple` key a `tuple`, a `set` key a `frozenset`). **A UDT key no longer does** (#3504): both a UDT key and a UDT value are a `cqlite.Udt`, matching Node, where a key and a value of the same CQL type always had the same host shape |
| `tuple<...>` | `tuple` (`tuple_to_py`) | `Array` (`list_to_array` — the `Value::Tuple` arm delegates to the list converter) | positional on both sides | **asymmetric discriminability**: Node **cannot distinguish `tuple<...>` from `list<T>`** — both are plain `Array`s. Python can (`tuple` vs `list`). Any comparison must therefore treat tuple and list as the same canonical shape |
| `frozen<T>` | unwrapped to the inner type's mapping | unwrapped to the inner type's mapping (`Value::Frozen(inner) => value_to_napi(ctx, inner)`) | as the inner type | **symmetric** — `frozen` is transparent on both sides |
| `udt` | `cqlite.Udt` — `.type_name`/`.keyspace` out of band, `.fields` holding declared fields only (`udt_to_py`, #3504) | object `{ typeName, keyspace, fields }` (`udt_to_object`, #3504); `fields` has a **null prototype** (`Object.create(null)`), so a field named `__proto__` is an own data property instead of reaching `Object.prototype`'s inherited accessor (R1-1) | Python: `.fields` is a `dict`, so field names collapse by value. Node: string property keys on `fields`, which inherit nothing | symmetric in SEMANTICS; the spelling differs by language convention (PyO3 exposes snake_case, napi-rs camelCases). Python additionally keeps the mapping protocol so `udt["street"]` works; Node does not mirror it, because re-flattening fields beside `typeName` is the defect #3504 removed |

Notes on identity that the table compresses:

1. **Well-formed Cassandra data contains no duplicate set elements or map keys**, so the
   collapse divergences above are not normally observable through a read path — and the canonical
   form is stable for such input. **Duplicates are OUT OF CONTRACT, not merely unusual:** given
   two structurally-equal **non-scalar** map keys, Python's `dict` collapses them (last value
   wins, one entry) while a Node `Map` compares object keys by reference and keeps **both**, so
   the two canonical forms differ in **length** and no amount of sorting reconciles them. That is
   instance **b-3** below; deduplicating to close it would be a behavior change (#3497).
2. **Ordering is not part of CQL set/map identity but is part of the host containers.**
   `frozenset` iteration is hash-ordered (and unstable across values/interpreters), JS `Set`
   and `Map` iteration is insertion-ordered, and Cassandra stores sets/map keys in clustering
   order. Canonicalization must sort; it must never assert an iteration order.
3. **`set<frozen<list<T>>>` stays hashable on the Python side** via
   `value_to_hashable_key` (`list`→`tuple`), so such a set is still a `frozenset` — **provided no
   UDT sits anywhere inside it**. The `list` fallback in `set_to_py` triggers on UDTs only, but
   since #3500 `contains_udt` traverses the WHOLE subtree, so a UDT at **any nesting depth** takes
   that fallback: `set<frozen<list<int>>>` is a `frozenset`, `set<frozen<list<frozen<udt>>>>` is a
   `list`. Hashable does not mean
   canonical: a `set<frozen<map<k,v>>>` element is projected to a *tuple of pairs*, a shape
   Node/the CLI never produce (instance **a-2** below).
4. **A UDT used as a MAP KEY now has the SAME shape as a UDT in value position (#3504).** Because
   `map_to_py` routes keys through `value_to_hashable_key`, such a key used to be a `frozenset` of
   `(field_name, value)` pairs (including `_type`/`_keyspace`) rather than the UDT the same value
   would be in value position, so it canonicalized to a sorted array of `[name, value]` pairs
   instead of the `{"_type": …}` object the CLI renders — instance **a-1**. That arm now projects
   to a `cqlite.Udt`, so key and value position agree and a `cqlite.Udt` is hashable whenever its
   field values are. The same projection is still why a nested `map` in a set element diverges
   (**a-2**, which #3504 did not touch).

**Empirical confirmation (2026-08-29).** The Python column was not only read from source but
observed against real Cassandra 5.0 fixtures: `test_collections.collection_table` returns
`frozenset` for `set<text>`/`set<int>`, `dict` for `map`, `list` for `list`;
`test_collections.collections_with_udts` returns a **`list`** for `contacts SET<FROZEN<contact_info>>`
(the fallback row) and a `dict` of UDT `dict`s for `emergency_contacts MAP<TEXT, FROZEN<contact_info>>`;
`test_types.cx_tuple_field_order` returns a Python `tuple` for `tuple<int, text, boolean>`. The Node
column is source-verified (`value_to_napi` and the three converters named above).

**Canonicalization rules (consumed by #1455).** The 3-way golden parity harness (#1455, Y1)
takes its canonicalization rules from this table; it does not re-derive them.

**The general principle — read this before the rules.** Canonicalization is reliable for a value
**iff, at every node of its value tree, every CQL type that could have produced that host shape
yields the same canonical form.** Two ways to satisfy it:

- the host shape **determines** the CQL type; or
- the several CQL types sharing that host shape **agree** on the canonical form — `tuple<...>` and
  `list<T>` are both a sequence and both canonicalize to an array, so their collision costs
  nothing.

Determination is therefore **sufficient but not necessary.** (An earlier formulation of this
section said "reliable exactly when the host shape uniquely determines the CQL type"; that is too
strong, and this document's own `tuple`/`list` row contradicted it.)

Because the criterion ranges over the **whole value tree**, **a container is benign only if all of
its descendants are.** A projected `list<T>` is benign for scalar `T` and **divergent** the moment
`T` contains a `map` or a `udt` — which is exactly instance **a-3**: a projected `list` that
diverges because of what is inside it. Never read a benign verdict on a container as unconditional.

Failure comes in exactly two **families** — that part is structural, a property of the two
mechanisms below — but the families are **GENERATIVE, not a taxonomy of a fixed set of cases**:
they produce an instance wherever a *projection position* (a set element or a map key) holds a
value whose canonical form the projection changes, and **nesting multiplies them**, because every
nested collection inside a projected value is another opportunity for the same two failures. So
the instance list further down is a floor, not a ceiling:

- **(a) Lossy projection.** `map_to_py` routes map KEYS, and `set_to_py` routes SET ELEMENTS,
  through `value_to_hashable_key`, which **discards the CQL type**. Losing the host TYPE is not
  by itself a defect, so the criterion is narrower than "a non-scalar in a projection position":

  > **A lossy projection diverges if and only if, at some node of the projected value, the
  > projected type's canonical form differs from the one Node/the CLI produce** — in practice: the
  > projection is benign while every node canonicalizes to a plain JSON array, and diverges at the
  > first node that does not.

  Worked through for every projected type. The three benign verdicts are **conditional on the
  descendants**, per the recursion above.

  > **This table SUMMARISES the projection; it does not DEFINE it.** The authoritative, exhaustive
  > arm list is `value_to_hashable_key` in `bindings/python/src/value_hashable.rs`, whose totality is
  > compiler-enforced (no `_ =>` arm, pinned by `#[deny(clippy::wildcard_enum_match_arm)]`). Never
  > assert *here* that an arm is missing or that a projection falls through — read the function.
  > This section has been corrected three times for restating that arm list from memory; one site
  > owns the fact.

  | Projected type | Python projection | Canonical form | Node/CLI canonical form | Verdict |
  |---|---|---|---|---|
  | `list<T>` | `List` arm → `tuple` | array | array | **benign iff every descendant is benign** — divergent when `T` holds a `map` (a-2 at depth). A nested `udt` used to make this row divergent (a-3) and no longer does: the `Udt` arm is type-preserving since #3504 |
  | `set<T>` | `Set` arm → `frozenset` (recursing through `value_to_hashable_key`, deliberately never re-entering `set_to_py` — #3500) | sorted array | sorted array | **benign iff every descendant is benign** — divergent when the element type holds a `map` (a-2). A nested `udt` neither diverges (#3504 made the projection type-preserving) nor RAISES any more: this arm recurses instead of falling through to `set_to_py`'s #804 `list` rendering, which is what used to produce `unhashable type: 'list'` (#3500) |
  | `tuple<...>` | the shared `List`/`Tuple` arm → `tuple` (one arm, same as `list<T>` — #3500) | array | array | **benign iff every descendant is benign** — divergent when an element holds a `map` (a-2); a nested `udt` neither diverges (#3504) nor raises (#3500) |
  | `map<k,v>` | `Map` arm → tuple of pairs | array of arrays | array of `{"key": …, "value": …}` | **DIVERGES** (a-2) |
  | `udt` | `Udt` arm → **`cqlite.Udt`** (identity out of band, field values recursively projected — #3504) | **object** | **object** | **benign** — the projection is type-PRESERVING, so a-1/a-3 are **CLOSED**. It used to project to a `frozenset` of `(name, value)` pairs, canonicalizing to an array of `[name, value]` where Node/the CLI produce an object |

  So family (a) now generates instances **only for `map`** in a projection position, plus anything
  that nests it. (Before #3504 `udt` was the second generator — instances a-1 and a-3, both now
  closed; the criterion is unchanged, `udt` simply stopped satisfying it.) **Benign projections, named explicitly so #1455 does not special-case
  them:** a `list`, `set` or `tuple` in a projection position loses its Python **host-type
  identity** (a `list` key arrives as a `tuple`) but its **canonical form is unchanged**, so it
  compares equal across bindings and needs no handling — **provided its elements are themselves
  benign.** A `map<frozen<list<int>>, v>` key is benign; a `map<frozen<list<frozen<map<k,v>>>>, v>` key is a-2 at depth. A `map<frozen<list<frozen<udt>>>, v>` key **was** a-3 and is now closed — #3504 made the `Udt` arm
  type-preserving. The live examples are **map keys** on purpose: since #3500 a *set* whose subtree
  holds a UDT never reaches this projection at all — `contains_udt` sends the column to
  `set_to_py`'s `list` branch — so a map key is where a nested projection is still observable. The
  defect is a *changed canonical form*, never a *lost host type*. (#3500's `TypeError` cases lived
  in these rows while the projection was partial; they are **FIXED** — see "Nested UDT shapes no
  longer RAISE" below — and were a totality bug in the binding, never a canonicalization
  divergence.)
- **(b) Host-shape collision.** Two different CQL types arrive as the same Python host type, so
  the normalizer cannot tell them apart: `set<frozen<udt>>` vs `list<T>` (both `list`); a `map`
  containing a literal `"_type"` key vs a `udt` (both a `dict` **before #3504**; a UDT is now a
  distinct `cqlite.Udt`, so only the `map` side of this pair is still unidentifiable); `tuple<...>` vs `list<T>` (both
  sequences — this one is **benign**, because the canonical form deliberately merges them anyway,
  Node having already erased the distinction).

Resolving (a) or (b) needs the **declared CQL type** threaded into normalization — i.e.
schema-aware normalization. That is a behavior change, out of scope for #1454, and tracked as
**#3497**, to which every non-benign instance below belongs.

**Rules that hold:**

- `list<T>`, `tuple<...>`, `set<*>` → a JSON **array**. Tuple and list canonicalize
  identically (Node cannot tell them apart).
- `set<scalar>` and `set<frozen<list|set>>` → a **sorted** array, hence order-insensitive.
  Sorting is available only because these arrive as a Python `frozenset`, a shape no `list<T>`
  can have. (`set<frozen<map>>` also sorts, but its *elements* do not canonicalize — instance
  **a-2**.)
- `map<k,v>` → a **sorted array of `{"key": k, "value": v}` objects**, keyed on the
  canonicalized key. This is also the CLI's JSON rendering of a map. It holds only where the host
  shape identifies the key type — a scalar key always, a non-scalar key only if no instance of
  family (a) or (b) applies to it (live cases: **a-2**, a `map` in key position, and **b-2**'s
  cell-level map site. **a-1** and **a-3**, both UDT-key cases, are CLOSED by #3504 — a UDT key is
  now a `cqlite.Udt` and identifies itself).
- `udt` → an object keyed by field name holding the **declared fields and nothing else**, i.e. the
  CLI's UDT JSON shape since #3629 (both JSON writers now share
  `cqlite-core/src/util/udt_json.rs`). There is **no `_type` entry and no `_keyspace` entry**: since
  #3504 the bindings inject neither, and since #3629 neither does the CLI, so a `_type` or
  `_keyspace` key present in the output is a genuine FIELD and survives. The shape matches
  `cassandra-5.0.8:.../UserType.java:261` (`toJSONString`), which emits declared fields only.
  Recognition is structural (`isinstance(v, cqlite.Udt)`), not a `"_type"` content sniff.

**Family (b), ENUMERATED: the Python host-shape lattice.** Family (b) — two CQL types sharing one
Python host shape — **can be closed, and this table closes it.** The set of host shapes is finite
and derivable: it is exactly the set of return shapes of `value_to_py`'s match arms (plus
`json_to_py`'s — numbers via `json_number_to_py` since #3505 — and `value_to_hashable_key`'s).
Every row below was derived from those arms in
`bindings/python/src/value.rs` and `bindings/python/src/value_hashable.rs`, not sampled, so the
table is **COMPLETE for the host shapes it
lists** — 17 shapes: #3504 added `cqlite.Udt`, and #3505 removed a *source* from `str`
(the deleted number-to-string fallback) without removing a shape. One outcome is deliberately not
a row: since #3505 a JSON number that no host type can represent exactly raises rather than
returning a value, and a refusal is not a host shape. Contrast with family (a), whose *instance* list ranges over an unbounded
nesting space and therefore cannot be closed. `Value::Frozen` adds no row: it recurses.

| Python host shape | CQL / value sources that produce it | Collision? | Canonical-form verdict |
|---|---|---|---|
| `None` | `Value::Null`, `Value::Tombstone` (deleted data → `None`), JSON `null`, a null UDT field | yes (4) | **benign** — every source canonicalizes to `null`. (A tombstone being indistinguishable from a genuine NULL is a *semantic* limitation of the row shape, not a canonicalization divergence.) |
| `bool` | `Value::Boolean`, JSON `true`/`false` | yes (2) | **benign** |
| `int` | `TinyInt`, `SmallInt`, `Integer`, `BigInt`, `Counter`, `Time` (ns since midnight, #1450), `Varint`, integral JSON number — **the full `i64` AND `u64` range since #3505** (Python's `int` is arbitrary precision, so `u64::MAX` is exact here) | yes (8) | **benign** — all canonicalize to a JSON number. `time` is compared against the CLI's `HH:MM:SS.nnnnnnnnn` parsed back to nanoseconds, so the collision costs nothing |
| `float` | `Float32`, `Float`, a JSON **float literal** | yes (3) | **benign**. This row said "non-integral JSON number" and was **FALSE until #3505**: an *integral* JSON number above `i64::MAX` also landed here, because `json_to_py` fell through `as_i64()` to a lossy `as_f64()`. The documentation described the intended boundary and the code did not match it; the code now does. Note the discriminator is the JSON **lexical form**, not the value — `1e19` is integral in value but a float literal, so it belongs here, while `10000000000000000000` is an integer literal and belongs in the `int` row |
| `str` | `Value::Text` (covers `text`/`ascii`/`varchar`), JSON string | yes (2) | **benign** at the canonical level. Until #3505 this row listed a third source, "a JSON number too large for `i64`/`f64`" (`json_to_py` falling back to `n.to_string()`), which was wrong twice over: nothing is "too large for `f64`" without `arbitrary_precision` — `as_f64()` always succeeds — so that arm was **unreachable**, and the reachable large-integer case became a rounded `float`, not a `str`. The fallback is deleted: a JSON number is now an exact `int`, a `float`, or a refusal, never a host-type shift to `str` |
| `bytes` | `Value::Blob` | no | — |
| `datetime.datetime` | `Value::Timestamp` | no | — |
| `datetime.date` | `Value::Date` | no | — |
| `uuid.UUID` | `Value::Uuid` (covers `uuid` and `timeuuid`) | yes (2) | **benign** — same canonical string |
| `decimal.Decimal` | `Value::Decimal` | no | — |
| `cqlite.Duration` | `Value::Duration` | no | — |
| `IPv4Address` / `IPv6Address` | `Value::Inet` | no | — |
| `list` | `list<T>`, a **UDT-bearing `set<…>`** (the `contains_udt` fallback, which since #3500 fires for a UDT at any nesting depth), **JSON array**. (A *projected* set adds no fourth source: `value_to_hashable_key`'s `Set` arm yields a `frozenset`, so it lands on the `frozenset` row below. It used to fall through to `set_to_py` and raise `unhashable type: 'list'` — measured still to raise after #3504, fixed by #3500's arm.) | yes (3) | **MIXED** — `set` vs `list` order-sensitivity is **b-1**; a **JSON array is BENIGN**, it stays an array and matches the CLI's array, so it is *not* a canonicalization divergence (its element ORDER is unverified, but that is **b-4**, which already covers every array, not a JSON-specific defect) |
| `frozenset` | `set<T>` (non-UDT elements), a **projected `set`** (`value_to_hashable_key`'s own recursive `Set` arm since #3500 — not a fall-through, and it never re-enters `set_to_py`) | yes (2) | **benign** — `set<T>` vs a projected `set` canonicalize identically. A **projected `udt`** was this row's third source and its only divergent one (**a-1**/**a-3**); since #3504 the `Udt` arm returns a `cqlite.Udt`, so it has left this row entirely |
| `tuple` | `tuple<...>`, a **projected `list<T>`** and a **projected `tuple<...>`** (both the shared `List`/`Tuple` arm since #3500, not a fall-through), a **projected `map<k,v>`** (`Map` arm → a tuple of pairs) | yes (4) | **MIXED** — benign for `tuple<...>`, a projected `list<T>` and a projected `tuple<...>` (all canonicalize to an array); **DIVERGENT for a projected `map`**, whose array-of-arrays is not the CLI/Node array of `{"key": …, "value": …}` — instance **a-2** |
| `dict` | `map<k,v>` (cell level), **JSON object**, and a **ROW** at row level. `udt` was a fourth source and is **no longer one** (#3504: `value_to_py`'s `Udt` arm returns a `cqlite.Udt`) | yes (3) | **DIVERGES** — a cell-level `map` carrying a literal `"_type"` key is still canonicalized as a UDT, which is **b-2**'s surviving cell-level-map site (the ambiguity is no longer *with a UDT*: it is an untyped `dict` misread by the `"_type"` sniff); the JSON object is **b-5**; the ROW source is disambiguated by the explicit `is_row_level` signal and is therefore **FIXED**, not a divergence |
| `cqlite.Udt` | `udt` (`value_to_py`'s `Udt` arm), a **projected `udt`** (`value_to_hashable_key`'s `Udt` arm) | yes (2) | **benign** — the two are identical BY CONSTRUCTION (one `build_udt`, differing only in the per-field converter), so both canonicalize to the CLI's UDT object. Added by #3504; no other CQL type produces this shape, which is what makes `isinstance(v, cqlite.Udt)` authoritative |

Reading rule: a shape with no collision needs no thought; a **benign** collision needs none either
(that is what benign means); the **three** rows carrying a divergent source — `list`, `tuple` (for
a projected `map` only) and `dict` — are the ones requiring #1455 to act, and their instances are
named in the next table. Since #3500 **no projection position raises for an unhashable shape**:
`value_to_hashable_key` is total over `Value` — every variant named, no `_ =>` arm — so a `udt` in
a projection position projects to a `cqlite.Udt`, a `set` to a `frozenset` and a `list`/`tuple` to
a `tuple`, and every value reaches the normalizer. **Scope that claim to SHAPE**, not to
infallibility: the function's JSON scalar arm delegates to `json_to_py`, which since **#3505**
can REFUSE a JSON number no host type represents exactly (`Beyond` → an exact `int` via `BigInt`,
else an `unsupported_format` error). A refusal is fail-closed and is never an unhashable value —
it is the direction #3500's own AC3 asked for — and it is UNREACHABLE in this build, because
without `arbitrary_precision` `serde_json` rounds such a literal to an `f64` in the PARSER before
any binding code runs (measured and test-asserted in `cqlite-ffi-common/src/json_number.rs`).
(A UDT-bearing *set* COLUMN
is a separate matter — `contains_udt` routes it to `set_to_py`'s `list` branch, which is row
**`list`** above, not a projection.)

**How each row was derived, and the verification trap to avoid.** A row answers "**which producers
can emit this host shape**", counted over BOTH `value_to_py`'s arms AND `value_to_hashable_key`'s
projections (`List`/`Tuple`→`tuple`, `Set`→`frozenset`, `Map`→tuple of pairs, `Udt`→`cqlite.Udt`,
`Json`→`json_to_hashable_key`, `Frozen`→recurse, every remaining SCALAR delegating to
`value_to_py` — read the function, per the note at the projection table) AND `json_to_py`'s arms.
**Confirming that all 26 `Value::`
variants appear somewhere in this section is NOT the same check** — that is *variant coverage*, and
it is strictly weaker: it passed while the `tuple` row was missing its projected-`map` source
(`Value::Map` appeared in the `dict` row, so the variant was "covered" while a row was incomplete).
Per-row source completeness is the property this table claims; verify it that way.

**Known instances (NOT exhaustive).** This table records the instances **identified so far**; it
is a floor, not a ceiling, and it cannot be closed — the nesting space the two families range
over is unbounded, so an enumeration could never be complete. **Do not read absence from this
table as absence of a problem:** check any newly-encountered nested shape against the principle
above, and add it here when found. Each listed instance is pinned by a test in
`TestCollectionIdentityContract` (`bindings/python/tests/test_cli_parity.py`) that records the
divergent shape as a **gap**, never as a desirable canonical form, and each is tracked by
**#3497** — except where the divergence happens *before* the normalizer runs (the **b-3**
collapse; #3500's `TypeError` cases were of the same pre-normalizer kind and are now **FIXED**,
see below), which no normalizer-level test can observe; for
b-3 the test pins what is observable, that the Python `dict` holds one entry where a Node `Map`
would hold two.

| # | Class | Instance | What actually happens | What #1455 must do |
|---|---|---|---|---|
| **a-1** | lossy projection | **UDT as a map KEY** (`map<frozen<udt>, v>`) | **CLOSED by #3504.** `value_to_hashable_key` used to project the key to a `frozenset` of `(field_name, value)` pairs (incl. `_type`/`_keyspace`), which canonicalized to a sorted array of `[name, value]` pairs while Node and the CLI render the same key as a UDT **object** — different in kind. The arm now projects to a **`cqlite.Udt` instance** (identity out of band, field values recursively projected), so the projection is type-PRESERVING and the key canonicalizes to the same UDT object the other two produce. Note the fix was NOT the declared type this row predicted: the loss was in the projection, not in the normalizer. **Reachability caveat, measured on `test-data/fixtures/issue_3504`:** a NON-frozen `map<frozen<udt>,v>` is multicell, so its key lives in the cell path and `parse_cell_path_key` decodes it to `Value::Blob` — only a `frozen<map<frozen<udt>,v>>` reaches this arm at all. | Nothing special. A UDT map key canonicalizes as a UDT object. A NON-frozen map's UDT key is a **blob** — a decode-level gap, not a canonicalization one. |
| **a-2** | lossy projection | **`map` inside a `set` element** (`set<frozen<map<k,v>>>`) | The element is projected to a **tuple of pairs**, so it canonicalizes to `[["a", 1]]`, while Node and the CLI render that nested map as `[{"key": "a", "value": 1}]`. The same nested map in *value* position goes through `value_to_py` → `dict` and canonicalizes correctly, so the divergence is specific to hashable-projection positions (set elements and map keys). | Treat a `map` nested in a set element (or map key) as **UNSUPPORTED**. The enclosing set still sorts; the element shape is what diverges. |
| **a-3** | lossy projection | **a UDT nested deeper inside a projected value** — e.g. the KEY of a `map<frozen<list<frozen<udt>>>, v>` | **CLOSED by #3504, with a-1 and by the same change.** `map_to_py` routes every key through `value_to_hashable_key` **unconditionally** (it has no `contains_udt` gate — a `dict` key must be hashable whatever it holds), and the `List` arm recurses, so the inner UDT used to be flattened into a `frozenset` of pairs and the key canonicalized to `[[["_keyspace", …], ["_type", …], ["street", …]]]` while Node and the CLI produce `[[{"_type": …, "street": …}]]`, an array holding a UDT **object**. Because the recursion now reaches a type-preserving `Udt` arm, every depth is fixed at once — the flip side of the generative property this row was recorded to demonstrate: a fix at the generator closes the whole family branch, and a fix at one depth would not have. **Scope note (#3500):** the example used to be a SET ELEMENT (`set<frozen<list<frozen<udt>>>>`), which no longer reaches the projection at all — `contains_udt` now traverses the whole subtree, so `set_to_py` takes its `list` branch and the inner UDT arrives as a `cqlite.Udt` inside nested `list`s, matching Node/the CLI (taken deliberately, AC1 over AC5). So the two changes close this row's two positions independently, and the map key above is the one where a hashable projection is unavoidable. Keep this consistent with `test_udt_nested_deeper_in_a_projection_position_now_canonicalizes` in `bindings/python/tests/test_cli_parity.py`. **The family is NOT closed**: the criterion is unchanged (a lossy projection diverges iff the projected type's canonical form is not a plain JSON array) and `map` still satisfies it, so **a-2 remains live**. | Nothing special for UDTs at any depth. Keep treating a **`map`** in a projection position as UNSUPPORTED (a-2). |
| **b-1** | host-shape collision — lattice row **`list`** | **any UDT-bearing `set<…>` vs `list<T>`** — `set<frozen<udt>>`, and since #3500 equally `set<frozen<list<frozen<udt>>>>`, `set<frozen<tuple<frozen<udt>, int>>>`, `set<frozen<set<frozen<udt>>>>`, … | Both are a plain Python `list` (`set_to_py`'s `contains_udt` fallback — which since #3500 fires for a UDT at **any nesting depth**, so this row is WIDER than a directly-frozen UDT; the verdict is unchanged), so the set cannot be sorted without also reordering genuine `list<T>` values, whose order is semantically meaningful. Consequence: this row's canonical form is **order-SENSITIVE** — two structurally-equal UDT sets whose elements are in different orders canonicalize to **different** arrays and compare **unequal**. | Handle the row explicitly: compare it order-insensitively itself, or declare it unsupported. Do **not** assume the canonical form has erased set ordering here. |
| **b-2** | host-shape collision (**injected control markers**) — lattice row **`dict`** | **`_type`/`_keyspace` injected into a namespace whose other keys are USER-CONTROLLED** — one class with **four known sites** (row columns, cell-level map keys, UDT field names, and the UDT-as-map-key projection) | Not one defect but a class: wherever an implementation injects those two markers, a user-chosen name can collide with them. Enumerated site by site, with status and reasoning, in **"The `_type`/`_keyspace` marker class"** directly below. | Per site — see the class entry. **Three of the four binding-side sites are now FIXED** (row dicts, UDT fields, the UDT-as-map-key projection); the **cell-level map** and **JSON object** sites remain open under **#3497**, which #3504 has handed the structural signal it lacked. |
| **b-3** | host-shape collision (key identity) | **duplicate structurally-equal NON-SCALAR map keys** | A Python `dict` cannot hold two such keys at all: they collapse by hash/`__eq__`, last value wins, one entry — while a Node `Map` compares object keys by **reference** and keeps **both**. The canonical forms then differ in **length**, which no sorting reconciles. Well-formed Cassandra data never produces duplicate map keys, so this is out of contract rather than a live read-path bug; the collapse happens in `map_to_py`, before any normalizer runs. Deduplicating the Node side (or rejecting the input) would be a behavior change. | Treat duplicate non-scalar map keys as **UNSUPPORTED**; do not compare lengths across bindings for such input. |
| **b-4** | host-shape collision (at COMPARISON time) | **`list<T>` *and* `tuple<...>` vs `set<T>` in `values_equal`** — so **element ORDER of BOTH a `list<T>` and a `tuple<...>` is NOT verified by the #319 parity comparison** | The canonical form merges sets and lists into arrays (by design), so the comparison layer cannot tell them apart either: `values_equal` tries an ordered comparison first and then falls back to an **unordered** (sorted) comparison. **Scope, stated from the guard's actual semantics rather than by example:** the guard is `not any(isinstance(v, dict) for v in py_val)`, which inspects only that level's **IMMEDIATE** elements, and the ordered path **recurses** — so the unordered fallback is applied **independently at EVERY array level whose immediate elements contain no `dict`, at any nesting depth.** A level that does hold `dict`s (a map-repr array, an array of UDT objects) is ordered-only *at that level*, but its nested arrays are still swallowed. Concretely `values_equal([[1, 2]], [[2, 1]])` is **True**, via the inner level. A reordered `list<int>` therefore compares **EQUAL** — and so does a reordered `tuple<...>`, because a tuple canonicalizes to the very same array (the benign `tuple`/`list` merge above). This contradicts BOTH the `list<T>` row ("positional; order preserved") and the `tuple<...>` row ("positional on both sides"): the canonical form deliberately merges the two, and the comparison layer then treats the merged shape as possibly-a-set. The fallback is a **deliberate accommodation with its reason recorded at the branch**: CQL `SET` columns are sorted by `_sort_key` on the Python side and emitted in Cassandra's internal byte-order by the CLI, so without it genuine set comparisons in the existing #319 suite would red. Removing it naively trades a false pass for a false failure; telling the two apart needs the declared type (#3497). | Do not rely on the #319 comparison to catch a `list<T>` or `tuple<...>` ordering regression **at any depth** — not the top level, and not nested inside another collection. A harness that must verify order has to compare those columns **ordered-only, recursively**, which means knowing which columns are lists/tuples, i.e. schema information. |
| **b-5** | host-shape collision — lattice row **`dict`** only | **JSON OBJECT cells** (`Value::Json`) | Scoped to objects deliberately: `json_to_py` maps a JSON **array** to a `PyList`, which canonicalizes to an array exactly as the CLI does — **benign**, not an instance (its ordering is b-4's, which covers all arrays). Only the **object** case diverges: it becomes a `PyDict` and is canonicalized as a **CQL map** (a sorted array of `{"key": …, "value": …}`) while the CLI keeps it an object — and a JSON object carrying a literal `"_type"` key is additionally read as a **UDT** (the `dict` lattice row has three cell-level sources, and JSON is the third). Reachability is deliberately NOT restated here: it is recorded in exactly one place — the `Value::Json` arm of `value_to_hashable_key` in `bindings/python/src/value_hashable.rs` — and this row asserts nothing about it. One thing this row must NOT be read as saying: that the blocker is *fixture absence*, i.e. that adding a corpus table would exercise this. It would not — see the single site. The shape divergence recorded here is what the row is for, independent of what delivers the value. | The **divergence** is JSON objects only. Excluding *all* `"json"`-comparator columns is still the recommendation, but state it as what it is — a **conservative policy** (a column's JSON values may be objects at some rows and not others, so per-value classification would be needed), **not** a claim that JSON arrays mis-canonicalize. Threading the declared type is the real fix (#3497). JSON *numbers* above `i64::MAX` used to land on the lossy `as_f64` path; that was **#3505** and is **FIXED** — both bindings now classify through the one shared `cqlite_ffi_common::json_number::classify_json_number`, so a `u64`-range integer reaches Python as an exact `int` and Node as a `BigInt`. It was a scalar-precision defect outside this contract's collection scope, and the `str`/`float`/`int` rows above are updated accordingly. |

**The `_type`/`_keyspace` marker class (instance b-2; issue #3504 — the UDT sites are now FIXED).**
Both bindings used to identify a UDT by **injecting two control markers into the same namespace that
carries user-controlled keys** — `udt_to_py` did `set_item("_type")`, `set_item("_keyspace")`, then
`set_item(field.name)` for every field, and `udt_to_object` did the identical thing with
`set_named_property`. Every position where those markers shared a namespace with user-chosen names was
a collision site. **We hit three of them in three separate review rounds before recognising the
pattern** (a fourth, the JSON object, came from the host-shape lattice above rather than from another
round — which is the point of enumerating), which is the tell that the sites were being patched one at
a time instead of the class being swept. **A FIFTH turned up in the review of the fix itself**, in
JavaScript's object model rather than in our own namespace (`__proto__`, below) — so the class is not
"our two markers" but *any control channel a field name can address*.

**#3504 removed the channel for the UDT sites rather than narrowing it.** Type identity is carried OUT
OF BAND: Python returns a `cqlite.Udt` whose `type_name`/`keyspace` live on the instance and whose
`fields` holds declared fields and nothing else; Node returns
`{ typeName, keyspace, fields }`. There is no longer a slot for a field name to compete for, so the
site is not "harder to hit" — it is **unexpressible**.

**A FIFTH SITE, found by review and worth recording because it is the same class in a DIFFERENT
namespace (#3504 R1-1).** Moving the fields into their own object removes OUR marker channel; it does
not remove the HOST's. On the Node side `fields` was a plain object, and a property assignment on a
plain object is a `[[Set]]` that consults the prototype chain — so a field named `__proto__` (as legal
as `_type`, via a quoted identifier) called `Object.prototype`'s inherited accessor instead of
becoming a field: measured on the fixture, a string value VANISHED and a null value REPLACED the
object's prototype. Fixed by building `fields` with `Object.create(null)`, which inherits nothing, so
NO name — not this one, not a future addition to `Object.prototype` — can reach an inherited accessor.
A special case on the literal string `__proto__` was explicitly rejected: that is picking a rarer
delimiter one level down. **Python has no analogous site**: `PyDict` insertion is a concrete store
consulting no descriptor chain, and the mapping namespace (`udt[...]`) is separate from the attribute
namespace (`udt.type_name`). The general lesson to carry: after removing your own shared channel, ask
which channel the HOST still shares with the data.

Subject: `test-data/fixtures/issue_3504/` (Cassandra-5.0.2-written, `CREATE TYPE collide ("_type"
text, "_keyspace" text, "__proto__" text, real_field int)`), asserted by
`bindings/python/tests/test_issue_3504_udt_field_namespace.py` and
`bindings/node/__test__/issue-3504-udt-field-namespace.test.js`. The known sites — the original four
in OUR namespace, plus the fifth in the host's:

| Site | User-controlled keys | Status |
|---|---|---|
| **Row dict** | column names | **FIXED.** A row with a column named `_type` used to be sniffed as a UDT, dropping a `_keyspace` column. `normalize_python_value` now checks the caller's explicit `is_row_level` signal **before** the `"_type"` content sniff. The signal is authoritative (every `is_row_level=True` call site normalizes a `row.to_dict()`, and a UDT is always a CELL, so it can only arrive with `is_row_level=False`) — which is also why the reorder cannot affect the UDT branch. **An unambiguous signal existed, so it was used instead of guessing.** #1455 needs no row-level handling. |
| **JSON object** | JSON keys | **OPEN — #3497.** A JSON object cell reaching Python as a `dict` (`json_to_py`) can carry a literal `"_type"` key of its own and is then canonicalized as if it were a UDT. Since #3504 that is no longer an ambiguity *with a UDT* — a UDT is a distinct `cqlite.Udt` — it is the normalizer's untyped-`dict` branch misreading a JSON object, the same residual as the cell-level map row. Instance **b-5**. For reachability see the single statement at the `Value::Json` arm of `value_to_hashable_key` — this row asserts nothing about it, and in particular does NOT claim that adding a fixture would reach it: the blocker is structural (schema validation refuses a `json` column), not fixture absence. |
| **Cell-level map** | map keys | **OPEN — #3497**, and now for a *narrower* reason. A `map<text, X>` holding a literal `"_type"` key is still canonicalized as if it were a UDT: an **object** instead of the documented `{"key": …, "value": …}` array, with a `"_keyspace"` entry dropped. What changed: the claim "no signal distinguishes a map from a UDT at cell level — both are a `dict`" is **no longer true**. Since #3504 a UDT is a distinct type, so `isinstance(v, cqlite.Udt)` (Python) is an **authoritative** structural signal — a distinct host type no other CQL value produces — and the normalizer's UDT branch is keyed on it. **Node's half is weaker and must not be described the same way:** `{typeName, keyspace, fields}` is a SHAPE, not a type, and a `Value::Json` object cell can carry those three keys (instance **b-5**), so on the Node side it remains a narrower structural SNIFF — better than `"_type"` (three co-occurring keys a UDT always has, rather than one marker) but not authoritative — no real UDT reaches the `"_type" in value` sniff any more. The surviving defect is therefore entirely on the MAP side: a `dict` whose CQL type is unknown to the normalizer. Closing it still needs the declared type threaded in (#3497); #3504 supplied the signal, not the dispatch. Until then treat such a map as **UNSUPPORTED**. |
| **UDT fields (ours)** | field names | **FIXED (#3504) — the channel is gone, not narrowed.** `_type`/`_keyspace` are legal quoted UDT field names, and because both bindings injected the markers *before* setting fields, the field used to OVERWRITE the metadata — symmetrically, in Python and Node — and the canonical rule then dropped `_keyspace` (the CLI omits it for UDTs) so such a field was **lost**. **Mechanism of the fix:** the field namespace is now a namespace of its own (`cqlite.Udt.fields` / `UdtValue.fields`) and identity rides outside it (`type_name`/`keyspace`, `typeName`/`keyspace`), so no field name can address the marker's slot — there is no slot. `udt["_type"]` now reaches the FIELD (Python) and `result._type` is `undefined` (Node); Node's `interface UdtValue` also lost the `[field: string]: Value` index signature that permitted the collision. A **NULL** `_type` field was a second, distinct failure mode (it nulled the injected type name) and is pinned too. The normalizer test that pinned the defect (`test_udt_field_named_keyspace_is_dropped`) now pins the FIX as `test_udt_field_named_keyspace_survives`. **The CLI-side residual is now FIXED too (#3629):** the CLI's JSON writer (`cqlite-cli/src/output/json.rs`) and its independent copy in `cqlite-core/src/query/result.rs` (`ToJson for Value`) no longer inject `_type` — both call one shared rule, `cqlite-core/src/util/udt_json.rs::udt_to_json_object`, which renders the declared fields and nothing else. The parity canonical form was re-baselined to that shape against the `sstabledump` golden (not against the CLI's previous output), so the suites no longer mirror a collision that no longer exists. |
| **UDT fields (the HOST's namespace, Node)** | field names | **FIXED (#3504 R1-1).** `fields` being a namespace of its own is not sufficient while it is a PLAIN object: `[[Set]]` consults the prototype chain, so a field named `__proto__` reached `Object.prototype`'s inherited accessor — a string value silently discarded, a null value replacing the prototype (both measured on the fixture, which now declares the field). `fields` is built with `Object.create(null)`; the fix is over the CLASS of inherited names, never a special case on this one. Python is unaffected — a `dict` store consults no descriptor chain and the mapping namespace is separate from the attribute namespace. **Not counted among the original four**: it is the same defect class in the HOST's namespace rather than in ours, which is exactly why it survived the sweep of ours. |
| **UDT-as-map-key projection** | field names | **FIXED (#3504).** `value_to_hashable_key`'s `Udt` arm used to push a pair for `_type`, then `_keyspace`, then one per field, so a field named `_type` yielded a **duplicate** `_type` pair inside the projected `frozenset` (the two pairs differ in value, so nothing dedupes them). Measured on the fixture before the fix: the projected key's pair names were `['_keyspace', '_keyspace', '_type', '_type', 'real_field']`. The arm now emits a **`cqlite.Udt`** — exactly one entry per declared field, none for the metadata. The property the injected pairs were incidentally supplying is preserved deliberately: `Udt.__eq__`/`__hash__` are over `(keyspace, type_name, fields)`, so two UDTs of different declared types with identical fields stay distinct map keys (`fcm` vs `ftm` in the fixture). This also CLOSES instances **a-1** and **a-3**, which it compounded. Python-only: Node keys a real JS `Map` by the object and needs no projection. |

**Why the obvious narrowings are wrong.** Requiring *both* `_type` and `_keyspace` before treating a
`dict` as a UDT, or picking a more obscure marker name, only chooses a **rarer delimiter on a channel
the data controls** — a legal map or UDT can carry any of them — which postpones the next site instead
of removing it. That is precisely `CLAUDE.md`'s control/data lesson: **when a decision is made from a
stream that carries both your markers and someone else's payload, remove the shared channel; do not
choose a rarer delimiter.** The real fix carries UDT identity **out of band** (a wrapper type, a
sidecar type map, or the declared CQL type threaded in) — three options are written up in **#3504**;
the canonicalization half is **#3497**. **#3504 chose the wrapper type and shipped it**, so this
paragraph now records a decision rather than three options: option (a) was adopted because the two
"reserved key" variants are *rarer delimiters* on a channel the data controls (any string is a legal
quoted CQL identifier, so no key is unreachable), and rejecting such a UDT at decode time would
refuse data Cassandra accepts and the CLI already reads — converting a rendering defect into a
permanent capability hole. Full rationale:
`openspec/changes/udt-field-namespace/proposal.md`.

**What made this class especially nasty: it WAS SYMMETRIC — and the two markers need DIFFERENT
oracles.** Python and Node injected the same markers in the same order and made the identical
mistake, so a **Python↔Node comparison could never reveal either half**: both sides agreed, and
agreement reads as parity. That is why #3504 needed a purpose-built Cassandra-written fixture
rather than a cross-binding comparison. The two markers also differ in how far the symmetry
reaches, and an earlier version of this section got it wrong by asserting "the CLI injects
nothing" — the CLI's JSON writer (`cqlite-cli/src/output/json.rs`, `Value::Udt` arm) inserts
`"_type"` **and then the fields**, exactly as the bindings used to. State after #3504:

| Marker | Who injects it (after #3504) | Detectable by | Valid oracle |
|---|---|---|---|
| **`_type`** | **nobody, since #3629.** The two bindings stopped injecting it in #3504; the CLI's JSON writer and `cqlite-core`'s `ToJson for Value` stopped in #3629 (one shared rule, `util/udt_json.rs`). | n/a — no injection left to detect | **`sstabledump` / the raw SSTable bytes** — and it remains the only valid oracle for a RENDERING rule, because the CLI is the parity suites' comparison oracle and cannot check itself. `cqlite-cli/tests/issue_3629_cli_udt_json_namespace.rs` and `cqlite-core/tests/issue_3629_core_tojson_udt_namespace.rs` assert against it, one per code copy. |
| **`_keyspace`** | **nobody.** The bindings no longer inject it and the CLI never did, so a `_keyspace` field is a plain field everywhere. | n/a — no injection to detect | **the CLI** (and `sstabledump`) |

**Primary source for the target shape** (a CQLite file:line would be circular):
`cassandra-5.0.8:src/java/org/apache/cassandra/db/marshal/UserType.java:261`
(`toJSONString`) iterates `for (int i = 0; i < types.size(); i++)` over `stringFieldNames`
alone, emitting **no type key and no keyspace key**, and appends the literal `null` where a
field's buffer is absent (line 280) — which independently confirms that the fixture's `id 3`
`"_type": null` is the CORRECT rendering of a null FIELD and not a residue of the marker.

**The marker-INJECTION class is now closed across the family, and the way it closed is the lesson.**
Scope, stated because two rows of the table above are still `OPEN — #3497`: what is closed is
CQLite *injecting* a marker into a UDT's field namespace. The mirror-image defect — a consumer
*sniffing* a `"_type"` key to decide an untyped `dict`/JSON object IS a UDT (instances b-5 and the
cell-level map) — is untouched by #3504 and #3629 and remains open under #3497. Before #3504 it
was *symmetric across the whole family*: Python, Node and the CLI made the identical mistake in the
identical order, so a Python↔Node comparison could never reveal either half and a binding↔CLI
comparison could not reveal the `_type` half. #3504 fixed the two bindings, which left one surface
with **two independent code copies** — and the parity suites' canonical form deliberately mirrored
the CLI, so they stayed green while the CLI's own output remained wrong. #3629 fixed both copies by
converging them onto ONE rule (`cqlite-core/src/util/udt_json.rs`), generic over the field-VALUE
renderer because the two writers differ materially in 11 other arms; convergence at the whole-writer
level would have been wrong. Confirmatory measurement on the #3504 fixture: `sstabledump` injects
**no** marker of its own — the non-colliding cell dumps as `{"label": …, "real_field": 7}` — i.e. the
authoritative reference tool already keeps type identity out of the field namespace, which is now
what every CQLite RENDERING surface does (the two JSON writers via `util/udt_json.rs`, and the
table/CSV path via `util/value_fmt.rs::format_udt`, which always did).

**BEHAVIOUR CHANGE (#3629), stated for consumers.** `cqlite --format json` (and
`QueryResult::to_json()`) used to emit a `_type` key inside every UDT object and now emits none:

```jsonc
// BEFORE (CQLite ≤ 0.16, `SELECT p FROM test_udt_collision.udt_collide`)
[ { "id": 1, "p": { "_type": "plain", "label": "no-colliding-field", "real_field": 7 } } ]

// AFTER (#3629) — and byte-identical to what `sstabledump` writes for the same cell
[ { "id": 1, "p": { "label": "no-colliding-field", "real_field": 7 } } ]
```

Anyone parsing `_type` out of CQLite's JSON must stop: the key is gone, and for a UDT that DECLARES a
field of that name it was never the type anyway (the field overwrote it). **`--format json` carries
no type channel BY DESIGN** — it is a bare array of row objects with no metadata envelope, and
`sstabledump` has none either, so a UDT's declared type is not recoverable from the JSON output.
Where the type name is needed, read it from the schema (`test-data/schemas/*.cql` /
`DESCRIBE TYPE`), or use a surface that carries types out of band: the Python binding's
`cqlite.Udt.type_name`/`.keyspace`, the Node binding's `typeName`/`keyspace`, or the Parquet/Arrow
export, whose schema is typed. Adding a metadata channel to `--format json` was considered and
rejected: inventing public output surface to satisfy a checker is the wrong trade (lead ruling on
#3629, precedent #3630), and the reference tool this format exists to match does not have one.

**And note what the golden CANNOT check.** For the colliding fixture, `sstabledump`'s flat
`{"_type": "user-supplied-type", …}` is textually IDENTICAL to what the old buggy binding injection
produced, so the committed `*-Data.db.jsonl` physical-dump parity is structurally blind to the site-3
rendering defect. It pins DECODE and proves the schema is legal Cassandra; the oracle for the
rendering rule is the spec's required shape, asserted at the binding surface.

This is exactly the shared-error blind spot `CLAUDE.md` describes for symmetric round-trip tests:
the oracle has to be the *other* implementation's bytes, never the same family's output — and for
`_type` the CLI is (still) inside the same family. The correction is recorded on **#3504**.

**Nested UDT shapes no longer RAISE — FIXED in #3500.** `contains_udt` and
`value_to_hashable_key` are now **total and exhaustive over `Value`**: neither has a `_ =>` arm, so
a future `Value` variant is a COMPILE error in both rather than a runtime `TypeError` on somebody's
data. `contains_udt` traverses the whole subtree (`List`/`Set`/`Tuple` elements, BOTH sides of a
`Map`, through `Frozen`), and `value_to_hashable_key` has recursive arms for `Tuple`, `Set`, `Map`,
`Frozen`, `Udt` and `Json`. The exhaustiveness is pinned mechanically by
`#[deny(clippy::wildcard_enum_match_arm)]` on both functions plus `json_to_hashable_key` (clippy runs
`-D warnings` in the gate).

What changed observably. The middle column is **measured**, not inferred — a point read per row with
each binding built into the same venv, on the committed fixture table
`test_udt_collision.udt_hashable_shapes` (#3504 R1-2) and
`test-data/fixtures/issue_3500_nested_udt`:

| CQL shape | Before #3504 | After #3504, before #3500 | After #3500 |
|---|---|---|---|
| `set<frozen<tuple<frozen<udt>, int>>>` | `TypeError: unhashable type: 'dict'` | **succeeded INCIDENTALLY** — `frozenset({(Udt, 10)})`; no arm was added, the fall-through to `value_to_py` merely happened to yield a hashable `tuple` once the UDT became a `cqlite.Udt` | Python `list` of `tuple`s, each `(cqlite.Udt, int)` — `contains_udt` now sees the nested UDT, so `set_to_py` takes its #804 `list` branch |
| `map<frozen<tuple<frozen<udt>, int>>, int>` | `TypeError: unhashable type: 'dict'` | succeeded incidentally, as above — `{(Udt, 20): 5}` | same shape, but now produced by the `Tuple` ARM rather than by a fall-through, so it no longer depends on `value_to_py`'s output happening to be hashable |
| `set<frozen<set<frozen<udt>>>>` | `TypeError: unhashable type: 'list'` | **still raised**, identically: the cause is #804's rendering of a UDT-bearing set as a Python `list`, not the UDT — the error text naming `'list'` rather than `'dict'` is what distinguishes the two causes | Python `list` of `list`s of `cqlite.Udt`s |
| `set<frozen<list<frozen<udt>>>>` | `frozenset` of `tuple`s of projected-UDT `frozenset`s | `frozenset` of `tuple`s of `cqlite.Udt`s (a-3 closed) | Python `list` of `list`s of `cqlite.Udt`s |
| `map<frozen<udt>, v>` **frozen** map, UDT key | `frozenset` of `(name, value)` pairs (a-1) | a **`cqlite.Udt`** — a-1 CLOSED, the projection is type-preserving | unchanged by #3500 |

The fourth row is a **DELIBERATE SHAPE CHANGE** (#3500 AC1 over AC5), not a regression. `contains_udt`
now sees the UDT under the inner `list`, so `set_to_py` takes its `list` branch for the whole
column. That REMOVES the nesting-dependent asymmetry that was the defect's own tell — the same UDT no
longer reached a `frozenset` at depth 2 while the ordinary `list` branch applied at depth 1. It
**narrows #3497 without closing it**: what remains in a projection position is a `map`
(instance a-2), whose tuple-of-pairs still differs in kind from the Node/CLI
`{"key": …, "value": …}` array, so schema-aware normalization remains the fix for that residue. The
UDT half of the residue is gone — #3504 made the projected UDT the same object the other two render.

One measurement worth keeping, because it falsified the obvious prediction: a UDT declaring a
`frozen<map<text,int>>` FIELD also projects successfully, even though `Udt.__hash__` hashes its field
values and a `dict` would be unhashable. **As measured at the time**, a collection field inside a
frozen UDT decoded to `Value::Blob`, so it arrived as hashable `bytes`.

**That decode gap is CLOSED by #3722** — one UDT-field decoder, total over `CqlType`, with no
`_ => Value::Blob` — so such a field now decodes to `{"a": 1}`. The projection still succeeds, but on
different grounds, verified by #3722: the subject column is a UDT-bearing set, so `set_to_py` takes its
#804 `list` branch and never hashes the `Udt`. The characterization pin in
`bindings/python/tests/test_issue_3504_udt_field_namespace.py` was updated accordingly.

**MULTICELL map keys were the remaining gap; #3612 CLOSED it.** A non-frozen (multicell)
`map<K, V>` carries each key in the cell PATH, and `parse_cell_path_key` used to decode cell-path
keys from a **scalar-only allowlist** (text/ascii/varchar, uuid/timeuuid, int, bigint/counter, date,
timestamp) with a `_ => Value::Blob` fallback, so a COMPOSITE multicell map key — a `frozen<udt>`,
`frozen<tuple<…>>` or nested collection — arrived as an opaque `Blob` and the Python side never saw
a structured key to project. That site is now
`cqlite-core/src/storage/sstable/reader/parsing/row_decoder/complex_column/cell_path_key.rs` and it
delegates to the structural decoder, so a multicell composite key decodes exactly as the **frozen**
spelling's always did and reaches `value_to_hashable_key` normally. The residual is narrower and
tracked separately: a nested element's declared width is not validated exactly (#3723), which only
bites on input Cassandra itself refuses to read.

Consequence for #1455: a harness no longer gets an exception for these shapes, so there is no
unsupported-shape error to special-case. It compares them like any other row, against the
after-column above.

Benign non-instance, recorded so it is not mistaken for a gap: `tuple<...>` vs `list<T>` collide
in the host shape on the Node side, but the canonical form merges them **by design**, so nothing
is lost.

The executable half of this contract is `normalize_python_value` **and its comparison layer
`values_equal`** in `bindings/python/tests/test_cli_parity.py` (issue #319) — see
`TestCollectionIdentityContract` there, which asserts the normalized shape of every row of the
table above **and** pins each known instance, so none can be mistaken for a solved case. The
former crash cases of #3500 ARE now pinned, in `bindings/python/tests/test_nested_udt_hashable.py`,
against the real SSTable fixture — end-to-end through the binding, which is where the defect lived
and where a pure normalizer test could never observe it.

**Temporal Type Precision Notes (Issue #299)**:

> **Note:** superseded in v0.13 (#1450) — the `duration`/`time` precision losses below no longer apply. `duration`→`cqlite.Duration(months, days, nanos)` and `time`→`int` (ns since midnight) are both exact/lossless. See the [v0.13 Migration Guide](./v0.13-migration-guide.md).

When converting CQL temporal types to Python, the following precision limitations apply:

1. **Duration Type Limitations**:
   - **Month Approximation**: Months are converted to days using 30-day approximation (1 month = 30 days)
   - This is a documented limitation because Python's `timedelta` does not support variable-length months
   - **Nanosecond Truncation**: Sub-microsecond precision is lost (`microseconds = nanoseconds / 1000`)
   - Python `timedelta` has microsecond precision; CQL `duration` has nanosecond precision

2. **Time Type Precision**:
   - CQL stores nanoseconds since midnight (`i64`)
   - Python `time` has microsecond precision
   - Conversion truncates sub-microsecond precision

3. **Timestamp Precision**:
   - CQL stores milliseconds since Unix epoch (`i64`)
   - Python `datetime` preserves millisecond precision (stored as microseconds)
   - All timestamps are converted to UTC timezone-aware datetime objects

These limitations match standard Python conventions and are consistent with how other database drivers handle temporal types.

---

## 6. CI/CD Infrastructure

### 6.1 Workflow Files

| Workflow | Purpose | Triggers |
|----------|---------|----------|
| `python-ci.yml` | Build/test Python wheels | Push to `bindings/python/**` |
| `python-release.yml` | Publish to PyPI | Tag `v*` |
| `node-ci.yml` | Build/test Node.js | Push to `bindings/node/**` |
| `node-release.yml` | Publish to npm | Tag `v*` |

### 6.2 Platform Matrix

| Platform | Python | Node.js |
|----------|--------|---------|
| Linux x64 | ✅ | ✅ |
| Linux ARM64 | ✅ | ✅ |
| macOS x64 | ✅ | ✅ |
| macOS ARM64 | ✅ | ✅ |
| Windows x64 | ✅ | ✅ |

### 6.3 Publishing

| Package | Registry | Auth Method |
|---------|----------|-------------|
| `cqlite` | PyPI | Trusted Publishing (OIDC) |
| `@cqlite/node` | npm | NPM_TOKEN secret |

---

## 7. Testing Strategy

### 7.1 Test Categories

| Category | Python | Node.js |
|----------|--------|---------|
| Unit (Rust) | ✅ | ✅ |
| Integration | pytest | Jest |
| sstabledump Parity | ✅ | ✅ |
| Type Conversion | ✅ | ✅ |
| Streaming | ✅ | ✅ |

### 7.2 Test Data

Use existing `test-data/datasets/sstables/` with JSONL reference files:
- 33 tables across 4 keyspaces
- All CQL types covered
- sstabledump parity validation

### 7.3 Coverage Targets

| Binding | Line Coverage | Branch Coverage |
|---------|---------------|-----------------|
| bindings/python | 80% | 70% |
| bindings/node | 80% | 70% |

---

## 8. Implementation Phases

### Phase 1: Foundation (Weeks 1-2)

**Both Bindings**:
- [ ] Create crate directory structure under `bindings/`
- [ ] Add workspace members to root `Cargo.toml`
- [ ] Configure Cargo.toml with workspace dependencies
- [ ] Implement basic `Database.open()` and `execute()`
- [ ] Implement error mapping
- [ ] Set up CI build verification

### Phase 2: Core API (Weeks 3-4)

**Python**:
- [ ] Complete sync API (`execute`, `execute_streaming`)
- [ ] Implement Value → Python conversion
- [ ] Add context manager support
- [ ] Implement `StreamingConfig`
- [ ] Write pytest integration tests

**Node.js**:
- [ ] Complete Promise-based API
- [ ] Implement Value → JS conversion
- [ ] Implement `StreamingConfig`
- [ ] Generate TypeScript definitions
- [ ] Write Jest integration tests

### Phase 3: Streaming & Polish (Weeks 5-6)

**Python**:
- [ ] Implement Python iterator for streaming
- [ ] sstabledump parity testing
- [ ] Performance profiling (<128MB target)
- [ ] Documentation and examples

**Node.js**:
- [ ] Implement `AsyncIterable` for streaming
- [ ] Add prepared statement support
- [ ] Complete TypeScript refinements
- [ ] sstabledump parity testing
- [ ] Documentation and examples

### Phase 4: Release (Week 7)

- [ ] Configure PyPI trusted publishing
- [ ] Configure npm publishing
- [ ] Create release workflow
- [ ] Write announcement/migration guide
- [ ] Final CI/CD verification

**Total Timeline**: 6-7 weeks

---

## 9. Risks and Mitigations

### 9.1 Technical Risks

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| Tokio runtime complexity | Medium | Medium | Use proven patterns from Polars, pydantic-core |
| Memory exceeds 128MB | High | Low | Streaming-first design, buffer limits |
| Type conversion edge cases | Medium | High | Property-based testing, fuzzing |
| Cross-platform build failures | Medium | Medium | Use official Docker images, matrix testing |

### 9.2 Schedule Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Python async more complex than expected | Low | Deferred to post-M4 (sync-first) |
| CI/CD debugging | Low | Use existing release.yml patterns |

### 9.3 Fallback Strategy

If sync Python proves insufficient:
- Ship sync-only for M4
- Add async support in subsequent release

---

## 10. WASM Deferral Notes

WASM bindings are deferred from M4 to M6 due to:

1. **Architectural complexity**: `cqlite-core` hardcodes `tokio::fs` in `platform/fs.rs`, requiring a `FileSystem` trait abstraction
2. **Scope reduction**: Removing WASM cuts M4 timeline from 10 weeks to 6-7 weeks
3. **Priority**: Python and Node.js have higher user demand

### 10.1 Minimal Future-Proofing

To prepare for M6 WASM work:

1. **Documentation**: Keep WASM section in original spec for M6 reference
2. **Compile guards**: Add `#[cfg(not(target_arch = "wasm32"))]` to new tokio-dependent code
3. **Issue tracking**: Create M6 milestone with WASM tasks

### 10.2 M6 WASM Scope (Deferred)

When M6 begins:
- Implement `FileSystem` trait abstraction
- Create `bindings/wasm/` crate
- Target ≤2MB compressed bundle
- Support IndexedDB + in-memory backends

---

## Appendix A: Existing Infrastructure

### A.1 Workspace Dependencies (Already Configured)

From root `Cargo.toml`:

```toml
# FFI Dependencies
libc = "0.2"
cbindgen = "0.26"

# WASM Dependencies (for M6)
wasm-bindgen = "0.2"
js-sys = "0.3"
web-sys = "0.3"
wasm-bindgen-futures = "0.4"
serde-wasm-bindgen = "0.6"
getrandom = { version = "0.2", features = ["js"] }
console_error_panic_hook = "0.1"
wee_alloc = "0.4"
```

### A.2 Core API Entry Points

- `cqlite-core/src/lib.rs`: `Database` struct
- `cqlite-core/src/query/result.rs`: `QueryResult`, `QueryResultIterator`
- `cqlite-core/src/types.rs`: `Value` enum
- `cqlite-core/src/error.rs`: `Error` enum
- `cqlite-core/src/config.rs`: `Config` struct

---

## Appendix B: Reference Projects

| Project | Language | Binding Tech | Notes |
|---------|----------|--------------|-------|
| Polars | Python | PyO3 | Excellent streaming patterns |
| pydantic-core | Python | PyO3 | Type conversion reference |
| SWC | Node.js | napi-rs | Large-scale native module |

---

## Appendix C: Success Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| PyPI downloads (30 days) | 1,000+ | PyPI stats |
| npm downloads (30 days) | 500+ | npm stats |
| Test pass rate | 100% | CI |
| sstabledump parity | 100% | Validation tests |
| Documentation coverage | All public APIs | Manual review |

---

## Appendix D: Review Resolution Summary

### Issues Raised and Resolved

| Issue | Resolution |
|-------|------------|
| WASM incompatibility (Gemini) | Deferred WASM to M6 |
| Async Python mandatory (Codex) | Updated to sync-first, async deferred |
| npm package naming (Codex) | Aligned to `@cqlite/node` |
| Repository layout conflict (Codex) | Updated to `bindings/` directory |
| StreamingConfig exposure (Codex) | Added explicit config to APIs |
| Python result not iterable (Codex) | Added `__iter__` to QueryResult |

---

*Last updated: January 2026*
