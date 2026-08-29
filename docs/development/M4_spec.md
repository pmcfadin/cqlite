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

3. **UDT Metadata Fields**: When converting CQL UDTs to Python `dict`, two metadata fields are added:
   - `_type`: The UDT type name (e.g., `"address_type"`)
   - `_keyspace`: The keyspace containing the UDT definition
   - All UDT fields are accessible by name as dict keys
   - Null UDT fields return Python `None`

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
`bindings/python/src/value.rs` — `list_to_py`, `set_to_py` (+ its `contains_udt` helper),
`map_to_py`, `tuple_to_py`, `value_to_hashable_key`. Node:
`bindings/node/src/value.rs` — `list_to_array`, `set_to_js_set`, `map_to_js_map`, and the
`Value::Tuple(items) => list_to_array(ctx, items)` arm of `value_to_napi`. Function names
are cited instead of line numbers because line numbers drift.

| CQL type | Python host type | Node host type | Identity semantics | Asymmetry |
|---|---|---|---|---|
| `list<T>` | `list` (`list_to_py`) | `Array` (`list_to_array`) | positional; order preserved on both sides; no dedupe | **symmetric** |
| `set<scalar>` | `frozenset` (`set_to_py`, non-UDT branch; elements go through `value_to_hashable_key`) | `Set` (`set_to_js_set`, `new Set(array)`) | Python: hash/`__eq__` value-equality. Node: SameValueZero — for scalars this is also value-equality | container type differs; **element identity agrees** for scalars. Iteration order differs: `frozenset` is hash-ordered, JS `Set` is insertion-ordered — canonicalize by sorting |
| `set<frozen<udt>>` | `list` — **fallback**, because UDTs become `dict`s and `dict` is unhashable (`set_to_py` takes this branch when `contains_udt` is true for any element) | `Set` of objects (`set_to_js_set`; no UDT fallback exists on the Node side) | Python: none — a `list` does not dedupe. Node: SameValueZero on **objects = reference identity**, so structurally-equal UDT elements are *not* deduped either | **asymmetric container**: Python degrades to `list`, Node keeps `Set`. Both are effectively order-preserving and non-deduping, so a set-of-UDT round-trips as a sequence on both sides. **Consequence for canonicalization:** because the Python side is a plain `list`, it cannot be sorted without also reordering genuine `list<T>` values, so this row's canonical form is **order-sensitive** — two structurally-equal UDT sets in different orders compare unequal (instance **b-1** in the canonicalization section below) |
| `map<k,v>` | `dict` (`map_to_py`); keys are the **hashable projection** `value_to_hashable_key` (arms: `list`→`tuple`, `map`→tuple of pairs, `udt`→`frozenset` of `(name, value)` pairs incl. `_type`/`_keyspace` sorted by field name, `frozen`→recurse; `set`/`tuple` have **no arm** and fall through to `value_to_py`, which still yields a hashable `frozenset`/`tuple` unless a UDT is nested — see #3500), values are the ordinary `value_to_py` | `Map` (`map_to_js_map`, `new Map(entries)`); **both** key and value use the ordinary `value_to_napi` | Python: keys collapse by hash/`__eq__` — writing an equal key overwrites, last-value-wins. Node: keys collapse by SameValueZero, so scalar keys collapse but **object keys (UDT / list / tuple keys) are compared by reference and never collapse** | **two asymmetries.** (1) *dict-key collapse*: structurally-equal non-scalar keys collapse in Python and survive as distinct entries on Node. (2) *key shape*: a Python map **key** is a hashable projection (a UDT key is a `frozenset` of pairs), while the same UDT as a map **value** is a `dict` — on Node a key and a value of the same CQL type have the same host shape |
| `tuple<...>` | `tuple` (`tuple_to_py`) | `Array` (`list_to_array` — the `Value::Tuple` arm delegates to the list converter) | positional on both sides | **asymmetric discriminability**: Node **cannot distinguish `tuple<...>` from `list<T>`** — both are plain `Array`s. Python can (`tuple` vs `list`). Any comparison must therefore treat tuple and list as the same canonical shape |
| `frozen<T>` | unwrapped to the inner type's mapping | unwrapped to the inner type's mapping (`Value::Frozen(inner) => value_to_napi(ctx, inner)`) | as the inner type | **symmetric** — `frozen` is transparent on both sides |
| `udt` | `dict` with `_type` + `_keyspace` metadata keys (`udt_to_py`) | object with `_type` + `_keyspace` properties (`udt_to_object`) | Python `dict`: keys collapse by value. Node object: string property keys | symmetric in shape; relevant here because it is what makes `set<frozen<udt>>` and UDT map keys behave as they do |

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
3. **`set<frozen<list<T>>>` etc. stay hashable on the Python side** via
   `value_to_hashable_key` (`list`→`tuple`), so such a set is still a `frozenset` — the `list`
   fallback in `set_to_py` triggers on UDTs only (`contains_udt`). Hashable does not mean
   canonical: a `set<frozen<map<k,v>>>` element is projected to a *tuple of pairs*, a shape
   Node/the CLI never produce (instance **a-2** below).
4. **A UDT used as a MAP KEY does not have the UDT value shape on the Python side.** Because
   `map_to_py` routes keys through `value_to_hashable_key`, a `map<frozen<udt>, v>` key is a
   `frozenset` of `(field_name, value)` pairs (including `_type`/`_keyspace`) rather than the
   `dict` the same UDT would be in value position — so it canonicalizes to a sorted array of
   `[name, value]` pairs, **not** to the `{"_type": …}` object the CLI renders. This is instance
   **a-1** in the canonicalization section below; the same projection is why a nested `map` in a
   set element diverges (**a-2**).

**Empirical confirmation (2026-08-29).** The Python column was not only read from source but
observed against real Cassandra 5.0 fixtures: `test_collections.collection_table` returns
`frozenset` for `set<text>`/`set<int>`, `dict` for `map`, `list` for `list`;
`test_collections.collections_with_udts` returns a **`list`** for `contacts SET<FROZEN<contact_info>>`
(the fallback row) and a `dict` of UDT `dict`s for `emergency_contacts MAP<TEXT, FROZEN<contact_info>>`;
`test_types.cx_tuple_field_order` returns a Python `tuple` for `tuple<int, text, boolean>`. The Node
column is source-verified (`value_to_napi` and the three converters named above).

**Canonicalization rules (consumed by #1455).** The 3-way golden parity harness (#1455, Y1)
takes its canonicalization rules from this table; it does not re-derive them.

**The general principle — read this before the rules.** Canonicalization is reliable **exactly
when the Python host shape uniquely determines the CQL type.** Where it does not, no
host-value-only normalizer can recover the difference, and the rule for that row does not hold.
Failure comes in exactly two **families** — that part is structural, a property of the two
mechanisms below — but the families are **GENERATIVE, not a taxonomy of a fixed set of cases**:
they produce an instance wherever a *projection position* (a set element or a map key) holds a
value whose canonical form the projection changes, and **nesting multiplies them**, because every
nested collection inside a projected value is another opportunity for the same two failures. So
the instance list further down is a floor, not a ceiling:

- **(a) Lossy projection.** `map_to_py` routes map KEYS, and `set_to_py` routes SET ELEMENTS,
  through `value_to_hashable_key`, which **discards the CQL type**. Losing the host TYPE is not
  by itself a defect, so the criterion is narrower than "a non-scalar in a projection position":

  > **A lossy projection diverges if and only if the projected type's canonical form is not a
  > plain JSON array.**

  Worked through for every projected type:

  | Projected type | Python projection | Canonical form | Node/CLI canonical form | Verdict |
  |---|---|---|---|---|
  | `list<T>` | `List` arm → `tuple` | array | array | **benign** |
  | `set<T>` | no `Set` arm — falls through to `value_to_py` → `set_to_py` → `frozenset` | sorted array | sorted array | **benign** |
  | `tuple<...>` | no `Tuple` arm — falls through to `value_to_py` → `tuple` | array | array | **benign** |
  | `map<k,v>` | `Map` arm → tuple of pairs | array of arrays | array of `{"key": …, "value": …}` | **DIVERGES** (a-2) |
  | `udt` | `Udt` arm → `frozenset` of `(name, value)` pairs | array of `[name, value]` | **object** | **DIVERGES** (a-1; a-3 when nested) |

  So family (a) generates instances **only for `map` and `udt`** in a projection position, plus
  anything that nests them. **Benign projections, named explicitly so #1455 does not special-case
  them:** a `list`, `set` or `tuple` in a projection position loses its Python **host-type
  identity** (a `list` key arrives as a `tuple`) but its **canonical form is unchanged**, so it
  compares equal across bindings and needs no handling. The defect is a *changed canonical form*,
  never a *lost host type*. (The two fall-through rows are also where #3500's `TypeError` cases
  live — see below — but that is a totality bug in the binding, not a canonicalization divergence.)
- **(b) Host-shape collision.** Two different CQL types arrive as the same Python host type, so
  the normalizer cannot tell them apart: `set<frozen<udt>>` vs `list<T>` (both `list`); a `map`
  containing a literal `"_type"` key vs a `udt` (both `dict`); `tuple<...>` vs `list<T>` (both
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
  family (a) or (b) applies to it (see **a-1**, **a-3** and **b-2** for known cases).
- `udt` → an object keyed by field name, with `_keyspace` dropped (the CLI omits it) and
  `_type` retained.

**Known instances (NOT exhaustive).** This table records the instances **identified so far**; it
is a floor, not a ceiling, and it cannot be closed — the nesting space the two families range
over is unbounded, so an enumeration could never be complete. **Do not read absence from this
table as absence of a problem:** check any newly-encountered nested shape against the principle
above, and add it here when found. Each listed instance is pinned by a test in
`TestCollectionIdentityContract` (`bindings/python/tests/test_cli_parity.py`) that records the
divergent shape as a **gap**, never as a desirable canonical form, and each is tracked by
**#3497** — except where the divergence happens *before* the normalizer runs (the **b-3**
collapse, and the #3500 `TypeError` cases below), which no normalizer-level test can observe; for
b-3 the test pins what is observable, that the Python `dict` holds one entry where a Node `Map`
would hold two.

| # | Class | Instance | What actually happens | What #1455 must do |
|---|---|---|---|---|
| **a-1** | lossy projection | **UDT as a map KEY** (`map<frozen<udt>, v>`) | `value_to_hashable_key` projects the key to a `frozenset` of `(field_name, value)` pairs (incl. `_type`/`_keyspace`), which canonicalizes to a sorted array of `[name, value]` pairs; Node and the CLI render the same key as a UDT **object**. Different in kind — nothing in the normalizer reconciles them. Reconstructing a UDT object from an anonymous `frozenset` of 2-tuples would be a shape guess, and no fixture table currently has a UDT map key. | Treat a UDT map key as **UNSUPPORTED**. Do not assume the projected frozenset shape is canonical. |
| **a-2** | lossy projection | **`map` inside a `set` element** (`set<frozen<map<k,v>>>`) | The element is projected to a **tuple of pairs**, so it canonicalizes to `[["a", 1]]`, while Node and the CLI render that nested map as `[{"key": "a", "value": 1}]`. The same nested map in *value* position goes through `value_to_py` → `dict` and canonicalizes correctly, so the divergence is specific to hashable-projection positions (set elements and map keys). | Treat a `map` nested in a set element (or map key) as **UNSUPPORTED**. The enclosing set still sorts; the element shape is what diverges. |
| **a-3** | lossy projection | **a UDT nested deeper inside a projected value** — e.g. `set<frozen<list<frozen<udt>>>>` | `value_to_hashable_key`'s `List` arm recurses, so the inner UDT is projected to a `frozenset` of `(field_name, value)` pairs: the element canonicalizes to `[[["_keyspace", …], ["_type", …], ["street", …]]]` — an array of `[name, value]` pairs — while Node and the CLI produce `[[{"_type": …, "street": …}]]`, i.e. an array holding a UDT **object**. This is not a new carve-out: it is a-1's projection reached one level deeper, and it is the concrete demonstration that **nesting generates instances**. Any further nesting of a projected non-scalar should be assumed to do the same until checked. | Treat a UDT nested anywhere inside a projection position as **UNSUPPORTED**. |
| **b-1** | host-shape collision | **`set<frozen<udt>>` vs `list<T>`** | Both are a plain Python `list` (`set_to_py`'s `contains_udt` fallback), so the set cannot be sorted without also reordering genuine `list<T>` values, whose order is semantically meaningful. Consequence: this row's canonical form is **order-SENSITIVE** — two structurally-equal UDT sets whose elements are in different orders canonicalize to **different** arrays and compare **unequal**. | Handle the row explicitly: compare it order-insensitively itself, or declare it unsupported. Do **not** assume the canonical form has erased set ordering here. |
| **b-2** | host-shape collision | **CELL LEVEL ONLY — `map<text, X>` containing a literal `"_type"` key vs a `udt`** | Both are a Python `dict`, and at cell level `"_type"` is the ONLY discriminator available, so the normalizer's `if "_type" in value:` branch classifies such a map as a UDT: it returns an **object** instead of the documented key/value array, and **drops a `_keyspace` entry** if the map has one. `"_type"`/`_keyspace` are legal `text` map keys, so this is a real (if unusual) false positive. Requiring `_keyspace` as well would only pick a rarer delimiter on an already-ambiguous channel — a legal map can carry both — so it is documented, not narrowed (see the control/data lesson in `CLAUDE.md`). **The ROW-LEVEL twin of this defect is FIXED, not documented** — see the note directly below. | Treat a **cell-level** `map<text, X>` whose keys may include `"_type"` as **UNSUPPORTED** until #3497 threads the declared type. Row level needs no handling. |
| **b-3** | host-shape collision (key identity) | **duplicate structurally-equal NON-SCALAR map keys** | A Python `dict` cannot hold two such keys at all: they collapse by hash/`__eq__`, last value wins, one entry — while a Node `Map` compares object keys by **reference** and keeps **both**. The canonical forms then differ in **length**, which no sorting reconciles. Well-formed Cassandra data never produces duplicate map keys, so this is out of contract rather than a live read-path bug; the collapse happens in `map_to_py`, before any normalizer runs. Deduplicating the Node side (or rejecting the input) would be a behavior change. | Treat duplicate non-scalar map keys as **UNSUPPORTED**; do not compare lengths across bindings for such input. |

**Where a structured signal exists, USE IT — the row-level half of b-2 is fixed, not documented.**
A ROW is also a Python `dict`, so a row with a column named `"_type"` (a legal quoted identifier)
used to be sniffed as a UDT, silently dropping a `"_keyspace"` column. That case is **closed**:
`normalize_python_value` now checks the caller's explicit `is_row_level` signal **before** the
`"_type"` content sniff. The signal is authoritative — every `is_row_level=True` call site
normalizes a `row.to_dict()`, and a UDT is always a CELL, so it can only arrive with
`is_row_level=False` — which is why the reorder cannot affect the UDT branch. The general rule,
the same one applied throughout this section: **a caller's structured signal beats guessing from
content.** It could not be applied at cell level because no such signal exists there, which is
exactly why b-2 survives as a cell-level limitation while the row-level case does not. #1455 needs
no row-level handling for this.

**Some nested shapes RAISE rather than diverge (issue #3500).** `contains_udt` and
`value_to_hashable_key` are **not total over `Value`**: `contains_udt` recurses only through
`Frozen`, and `value_to_hashable_key` has arms for `List`/`Map`/`Frozen`/`Udt` but **none for
`Tuple` or `Set`** (both fall through to `value_to_py`). So for shapes such as
`set<frozen<tuple<frozen<udt>, int>>>` and `set<frozen<set<frozen<udt>>>>`, `set_to_py` commits to
the `frozenset` path — `contains_udt` never sees the nested UDT — and the fall-through then yields
an unhashable `dict`/`list`, raising `TypeError: unhashable type` **inside the binding, before any
normalizer runs**. That is a production `value.rs` defect, tracked separately as **#3500** (#3497
is shape-only), and fixing it is out of scope for #1454, which forbids `value.rs` edits.
Consequence for #1455: a harness hitting one of these gets an **exception, not a mismatch**, and
must not record it as a parity failure — it is an unsupported-shape error to be reported as such.

Benign non-instance, recorded so it is not mistaken for a gap: `tuple<...>` vs `list<T>` collide
in the host shape on the Node side, but the canonical form merges them **by design**, so nothing
is lost.

The executable half of this contract is `normalize_python_value` in
`bindings/python/tests/test_cli_parity.py` (issue #319) — see
`TestCollectionIdentityContract` there, which asserts the normalized shape of every row of the
table above **and** pins each known instance, so none can be mistaken for a solved case. The
crash cases of #3500 are deliberately not pinned there: they raise inside the binding, so the
normalizer never sees them.

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
