# CLAUDE.md

Guidance for Claude Code when working with CQLite.

## Project Overview

CQLite is a Rust library for local Apache Cassandra SSTable access. It reads Cassandra 5.0 data files without cluster dependencies.

**Status**: M4 Complete (Jan 2026) - Core reading (M1), CLI (M2), Output Writers (M3), and Python Bindings (M4) are production-ready. Next: M5 (Write Support).

## Documentation

### Primary Reference
**SSTable Format**: `docs/sstables-definitive-guide/README.md` - Single source of truth

Key chapters:
- Ch.5: Data.db Format (rows, flags, V5CompressedLegacy)
- Ch.6: Index.db/Summary.db (partition lookups)
- Ch.17: BTI Formats (trie indexes)
- Appendix B: Encoding Cheat Sheet (VInt, flags)
- Appendix F: Known Limitations (what doesn't work yet)

### Project Documentation
- `docs/archive/issues/INDEX.md` - Historical issue investigations
- `test-data/validation-matrix.md` - Current test pass rates

## Available Skills (Auto-invoked)

Skills in `.claude/skills/` activate automatically when relevant:

| Skill | Use Case |
|-------|----------|
| `sstable-parsing` | Binary format parsing, hex dumps, compression |
| `cql-type-system` | CQL type deserialization |
| `rust-patterns` | Zero-copy, async I/O, memory efficiency |
| `ci-cd-validation` | Pre-push checks, CI requirements |
| `test-data-management` | Test SSTable generation, validation |

## Available Subagents

Subagents in `.claude/agents/` for specialized tasks:

| Agent | Model | Purpose |
|-------|-------|---------|
| `sstable-developer` | sonnet | SSTable implementation, format debugging |
| `rust-reviewer` | sonnet | Code review, quality enforcement |
| `test-validator` | haiku | Test execution, sstabledump parity |

## Essential Commands

```bash
# Build
cargo build

# Test (requires test data)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets cargo test --package cqlite-core

# Clippy (CI mode - must pass)
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features

# Format
cargo fmt

# Smoke test all tables
bash test-data/scripts/smoke-test-all-tables.sh

# Run CLI
cargo run --package cqlite-cli -- <command>

# One-shot query mode (Issue #223)
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json

# Python bindings build and test
cd bindings/python && maturin develop  # Development build
cd bindings/python && maturin build --release  # Release wheel

# Run Python tests - fast tests only (default, Issue #331)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -v

# Run all Python tests including slow (CLI parity, performance)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets RUN_SLOW_TESTS=1 pytest bindings/python/tests -v

# Run only slow tests (CLI parity and performance)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -m slow -v

# Exclude slow tests explicitly
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -m "not slow" -v

# Python usage example
python3 -c "
import cqlite
with cqlite.open('test-data/datasets/sstables', schema='test-data/schemas/basic-types.cql') as db:
    for row in db.execute('SELECT * FROM test_basic.simple_table LIMIT 5'):
        print(row.to_dict())
"

# Node.js bindings build and test (Issue #290, #296, #306)
cd bindings/node && npm install && npm run build  # Build native module
cd bindings/node && npm test                       # Run all tests (Jest)
cd bindings/node && npm run test:watch             # Watch mode for development
cd bindings/node && npm run test:coverage          # Run with coverage report

# Node.js usage example (Issue #296 - Phase 2 complete)
node -e "
const { Database } = require('@cqlite/node');
(async () => {
  const db = await Database.open('test-data/datasets/sstables', {
    schema: 'test-data/schemas/basic-types.cql'
  });
  const result = await db.execute('SELECT * FROM test_basic.simple_table LIMIT 5');
  console.log('Rows:', result.rowCount);
  for (const row of result.rows) {
    console.log(row.name);
  }
  await db.close();
})();
"
```

### CLI Output Format Precedence

- `--out` takes precedence over `--format` when both specified
- `--query` is an alias for `--execute` (`-e`)
- Environment variable: `CQLITE_OUT` sets default output format

### CLI Modes (Issue #242)

The CLI supports three modes with enhanced status display:

**TUI Mode** (`cqlite tui`): Full terminal UI with status bar showing:
```
Health: OK | Mem: 24.5 MB | Data: 1.2 GB | Status: Ready | Mode: EDIT
```

**REPL Mode** (`cqlite repl`): Interactive shell with status line:
```
[OK] Mem: 24.5 MB | Data: 1.2 GB
cqlite>
```

**One-shot Mode**: Direct query execution with `--execute` or `--query` flags.

Status metrics refresh every 5 seconds. Status line disabled for piped output.

## Workspace Structure

```
cqlite-core/     # Core library (SSTable parsing, query engine)
cqlite-cli/      # Command-line interface
bindings/python/ # Python bindings (PyO3) - M4 complete
bindings/node/   # Node.js bindings (napi-rs) - Phase 2 complete (Issue #296)
test-data/       # Real Cassandra 5.0 SSTables for testing
tools/           # sstabledump-validator, format-validator
```

**Planned (M6)**: `bindings/wasm/` (WebAssembly bindings)

### Python Bindings Structure

```
bindings/python/
├── src/                    # PyO3 binding implementation
│   ├── lib.rs             # Module initialization
│   ├── database.rs        # Database class (open/close/execute)
│   ├── result.rs          # QueryResult, Row, StreamingIterator
│   ├── value.rs           # CQL to Python type conversions
│   ├── error.rs           # Exception mapping
│   ├── config.rs          # StreamingConfig, presets
│   ├── runtime.rs         # Tokio runtime management
│   ├── prepared.rs        # PreparedStatement bindings
│   └── stats.rs           # DatabaseStats bindings
├── python/cqlite/
│   ├── __init__.py        # Python package wrapper
│   └── __init__.pyi       # Type stubs for IDE support
├── tests/                 # 17 test files, 360+ tests
│   └── conftest.py        # Shared fixtures and path constants (Issue #330)
├── pyproject.toml         # Maturin build configuration
└── Cargo.toml             # Rust dependencies
```

### Node.js Bindings Structure (Issue #290, #296, #297, #303, #312)

```
bindings/node/
├── src/
│   ├── lib.rs             # napi-rs entry point, module exports
│   ├── database.rs        # Database class, QueryResult, ColumnInfo
│   ├── streaming.rs       # StreamingResult for async iteration (Issue #305)
│   ├── value.rs           # CQL to JavaScript type conversions
│   └── error.rs           # Error mapping (cqlite_core::Error → napi::Error)
├── lib/
│   ├── index.js           # Enhanced entry point with error wrapper
│   ├── index.d.ts         # Complete TypeScript definitions (Issue #312)
│   └── error-wrapper.js   # JavaScript error enhancement layer
├── __test__/
│   ├── setup.js           # Jest setup with centralized paths (Issue #306)
│   ├── helpers.js         # Test utilities (openDatabase, skipIfNoDatasets)
│   ├── parity-utils.js    # JSONL parsing, type normalization utilities (Issue #307)
│   ├── parity.test.js     # sstabledump parity tests - 39 tests (Issue #307)
│   ├── types.test.js      # Comprehensive type conversion tests - 44 tests (Issue #308)
│   ├── typescript-definitions.test.js  # TypeScript definitions validation - 68 tests (Issue #312)
│   ├── smoke.test.js      # Basic import tests (4 tests)
│   ├── config.test.js     # StreamingConfig tests (8 tests)
│   ├── database.test.js   # Database API tests (8 tests)
│   ├── error.test.js      # Error mapping tests (8 tests)
│   ├── value.test.js      # Value type conversion tests (16 tests)
│   ├── result.test.js     # QueryResult and ColumnInfo tests (10 tests)
│   └── streaming.test.js  # Streaming iterator tests - 19 tests (Issue #305)
├── jest.config.js         # Jest configuration (Issue #306)
├── Cargo.toml             # napi-rs dependencies
├── build.rs               # napi build script
├── package.json           # npm package config (@cqlite/node)
├── index.js               # Generated platform loader
└── index.d.ts             # Generated TypeScript definitions (auto-generated by napi-rs)
```

**Status**: Phase 3 (Streaming) complete (Issue #305).
- `Database.open(dataDir, options?)` - Open database with optional schema
- `Database.execute(query)` - Execute CQL query, returns QueryResult with columns metadata
- `Database.executeNative(query)` - Execute with native JS types (BigInt, Date, Buffer, Set, Map)
- `Database.executeStreaming(query, config?)` - Execute with async iteration (Issue #305)
- `Database.getStats()` - Get database statistics
- `Database.close()` - Idempotent close
- Error properties: `code`, `category`, `isRecoverable` on all thrown errors

**QueryResult Fields**:
- `rows: object[]` - Result rows as JavaScript objects
- `rowCount: number` - Number of rows returned
- `executionTimeMs: number` - Query execution time in milliseconds
- `columns: ColumnInfo[]` - Column metadata array

**ColumnInfo Fields** (Issue #303):
- `name: string` - Column name
- `dataType: string` - CQL data type (e.g., "Text", "Integer", "List")
- `nullable: boolean` - Whether column can be null
- `position: number` - Column position (0-indexed)
- `tableName: string | null` - Original table name (for joined queries)

**Error Codes** (Issue #297):
| Code | Category | Description |
|------|----------|-------------|
| `IO` | System | I/O errors (file access, memory, timeout) |
| `SCHEMA` | Schema | Schema/table errors |
| `QUERY` | Query | Query execution, CQL syntax errors |
| `PARSE` | Data | Binary format parsing, type conversion |
| `CONFIG` | Configuration | Configuration errors |
| `STORAGE` | Storage | Storage engine errors |
| `NOT_FOUND` | NotFound | Resource not found |
| `INVALID_INPUT` | Logic | Invalid operation/state |

**Streaming Support** (Issue #305): AsyncIterator for memory-efficient large result sets.
- `Database.executeStreaming(query, config?)` returns `AsyncIterable<Row>`
- Use with `for await...of` loop for natural JavaScript iteration
- Memory bounded by `StreamingConfig` (default ~11MB peak: 1024 rows buffer + 10K chunk)
- Automatic resource cleanup on break/error/completion
- 19 tests in `streaming.test.js` covering iteration, config, termination, errors

**Test Infrastructure** (Issue #306): Jest-based testing with centralized setup.
- 255 tests across 11 test files, all passing
- Coverage thresholds: 80% lines, 65% branches, 80% functions
- Centralized path handling in `__test__/setup.js` (matches Python conftest.py)
- Test helpers in `__test__/helpers.js`: `openDatabase()`, `skipIfNoDatasets()`

**Type Conversion Tests** (Issue #308): Comprehensive CQL type testing.
- 44 tests covering all primitive, text, binary, temporal, and collection types
- Precision tests for BigInt, decimal, and nanosecond time values
- Cross-realm safe type checking with `isDate()`, `isMap()`, `isSet()` helpers
- Coverage of UDTs, nested collections, and frozen types

**Parity Tests** (Issue #307): sstabledump validation for all 33 tables.
- Tier 1: Row count parity validation (33/33 tables, 3 with known issues)
- Tier 2: Column and type validation for representative tables
- JSONL utilities in `__test__/parity-utils.js` for parsing and comparison
- Run with: `npm run test:parity` (requires CQLITE_DATASETS_ROOT)

**TypeScript Definitions** (Issue #312): Complete type-safe definitions.
- 68 tests validating type definitions in `typescript-definitions.test.js`
- No `any` types - uses `Value` union type for all CQL-to-JS mappings

**Publishing Validation** (Issue #314): npm publishing readiness tests.
- 31 tests in `publish.test.js` validating package.json, napi config, files array
- Tarball creation test (with `RUN_SLOW_TESTS=1`)
- Dry-run verification step in release workflow
- Includes `Duration`, `UdtValue`, `Row`, `NativeQueryResult` interfaces
- Comprehensive JSDoc on all exports with `@example`, `@param`, `@throws` tags
- Types configured in package.json: `"types": "lib/index.d.ts"`

**CI Integration** (Issue #291): Tests run automatically via `node-ci.yml` on every PR.
- Build matrix: 5 platforms (Linux x64/ARM64, macOS x64/ARM64, Windows x64)
- Tests run on 3 native platforms (ARM64 Linux and x64 macOS are build-only)
- Release workflow: `node-release.yml` publishes to npm on version tags

### Key Source Paths

```
cqlite-core/src/
├── storage/sstable/           # SSTable parsing
│   ├── reader/parsing/        # Format parsers
│   │   └── v5_compressed_legacy.rs  # Main V5 parser
│   ├── bti/                   # BTI index support
│   └── row_cell_state_machine.rs    # OA format parser
├── parser/                    # SSTable binary format parsing
├── cql/                       # CQL text parsing (query strings → AST)
├── query/                     # Query engine (M2+)
└── schema/                    # Schema management
```

## Development Standards

### No-Heuristics Mandate (Issue #28)
- Use authoritative metadata only, no guessing
- Schema-aware decoding when schema present
- Legacy heuristics behind opt-in feature flag

### Code Quality
- `RUSTFLAGS="-D warnings"` must pass
- No `unwrap()`/`expect()` in library code
- Use `thiserror` for errors
- Memory target: <128MB for large files

### Testing
- Integration tests use real SSTable data only
- Validate against sstabledump output
- JSONL reference files for parity checking

### Python Bindings Thread Safety (Issue #311)
- **Database handle**: Thread-safe via `Arc<Database>` + `AtomicBool`
- **Close**: Idempotent, safe to call from multiple threads
- **GIL release**: All async operations release Python GIL (`py.allow_threads()`)
- **Streaming**: Each thread can use its own `StreamingIterator`
- **Known issue**: Concurrent queries on same database may have race condition in schema metadata access (requires warm-up query before parallel access)

### Python/CLI Output Parity (Issue #319)
- Python `db.execute()` and CLI `--out json` produce equivalent data
- Type differences: Python uses native types (datetime, UUID, bytes), CLI uses JSON strings
- Normalization required for comparison (see `bindings/python/tests/test_cli_parity.py`)
- Test coverage: All 33 tables validated for CLI parity

### Python E2E Test Architecture (Issue #323)

**Primary E2E Tests** (`bindings/python/tests/`):
- `test_parity.py`: Validates all 33 tables against JSONL golden files
  - `TestRowCountParity`: Row count validation per keyspace (31/33 passing, 2 xfail)
  - `TestValueParity`: Cell-level value validation for representative tables
  - `TestE2ESummary`: Explicit assertion that all 33 tables pass E2E validation
- `test_cli_parity.py`: Validates Python vs CLI output equivalence for all 33 tables

**Test Data**:
- JSONL golden files: `test-data/datasets/sstables/{keyspace}/{table}-{hash}/nb-1-big-Data.db.jsonl`
- Generated by Cassandra `sstabledump` tool

**Known Issues** (tracked as XFail):
- `static_columns_table`: Static column duplication in query results (200 vs 100 rows)
- `typed_collections_table`: V5CompressedLegacy cell extraction failure (1 vs 50 rows)

**CI Integration**: Tests run automatically via `python-ci.yml` on every PR.

## Test Data

Location: `test-data/datasets/sstables/`

| Keyspace | Tables | Purpose |
|----------|--------|---------|
| test_basic | 8 | Simple types |
| test_collections | 8 | Lists, sets, maps |
| test_timeseries | 9 | Time-series patterns |
| test_wide_rows | 8 | Wide partitions |

**Current pass rate**: 100% (33/33 tables passing as of Dec 2025)

### Fetching Test Data

The git repository contains only JSONL reference files (for validation).
To run integration tests with real SSTable data, fetch the binary files:

```bash
bash test-data/scripts/fetch-datasets.sh
```

Without Data.db files, query tests will pass but return 0 rows.

## Feature Flags

Default (cqlite-core): `all-compression`, `state_machine`

| Feature | Description | In Defaults? |
|---------|-------------|--------------|
| `all-compression` | LZ4, Snappy, Deflate, Zstd support | Yes |
| `state_machine` | Query engine and discovery | Yes |
| `cli-helpers` | CLI-specific ingestion/REPL API (Issue #249) | No |
| `metrics` | Performance metrics collection | No |
| `experimental` | Experimental features | No |

```bash
# Minimal build (pure library, no query engine)
cargo build --package cqlite-core --no-default-features --features all-compression

# Build with CLI helpers for integration testing
cargo build --package cqlite-core --features cli-helpers
```

## Troubleshooting

**Missing test data**: Set `CQLITE_DATASETS_ROOT=$PWD/test-data/datasets`

**Query tests return 0 rows**: Fetch SSTable Data.db files with `bash test-data/scripts/fetch-datasets.sh`

**Clippy failures**: Run with `RUSTFLAGS="-D warnings"` to match CI

**Parsing issues**: Check `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`

**Python bindings won't build**: Ensure Rust 1.85+ and maturin are installed:
```bash
pip install maturin
rustup update
```

**Python import errors**: Verify Python 3.9+ and rebuild bindings:
```bash
python3 --version  # Must be 3.9+
cd bindings/python && maturin develop
```

**Python tests skip or fail**: Ensure test data is available:
```bash
export CQLITE_DATASETS_ROOT=$PWD/test-data/datasets
bash test-data/scripts/fetch-datasets.sh
```

## Resources

- **Definitive Guide**: `docs/sstables-definitive-guide/`
- **Project Issues**: https://github.com/pmcfadin/cqlite/issues
- **Cassandra Source (local)**: `~/local_projects/cassandra` - Full Cassandra 5.0 codebase
- **Cassandra Source (remote)**: https://github.com/apache/cassandra/tree/cassandra-5.0.0
