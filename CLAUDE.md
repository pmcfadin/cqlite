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

# Run Python tests (requires test data)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -v

# Python usage example
python3 -c "
import cqlite
with cqlite.open('test-data/datasets/sstables', schema='test-data/schemas/basic-types.cql') as db:
    for row in db.execute('SELECT * FROM test_basic.simple_table LIMIT 5'):
        print(row.to_dict())
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
bindings/python/ # Python bindings (PyO3) - M4
test-data/       # Real Cassandra 5.0 SSTables for testing
tools/           # sstabledump-validator, format-validator
```

**Planned (M5+)**: `bindings/node/` (Node.js bindings - M4), `bindings/wasm/` (WebAssembly - M6)

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
├── tests/                 # 16 test files, 355 tests
├── pyproject.toml         # Maturin build configuration
└── Cargo.toml             # Rust dependencies
```

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
