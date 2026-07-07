# CQLite Developer Cookbook

Command reference and usage examples moved out of `CLAUDE.md` (issue #2101) to keep the per-session
agent context lean. `CLAUDE.md` holds the rules; this file holds the recipes.

For the agent gate (`scripts/agent-gate.sh`) see `CLAUDE.md` (contract) and
`docs/development/gate-ops.md` (deep mechanics). For source layout see the
[source map](https://pmcfadin.github.io/cqlite/agents-developing/source-map/).

## Profiling loop

See `docs/profiling.md`.

```bash
./scripts/profile.sh baseline        # save criterion baseline
./scripts/profile.sh flame           # CPU flamegraphs (pprof, works in containers)
./scripts/profile.sh heap            # dhat heap profile vs <128MB budget
./scripts/profile.sh bench && ./scripts/profile.sh compare   # re-measure vs baseline
./scripts/profile.sh report          # ranked bottleneck report + history.jsonl ledger
```

## CLI

```bash
# Run CLI
cargo run --package cqlite-cli -- <command>

# One-shot query mode (Issue #223)
cargo run --package cqlite-cli -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json
```

### Output format precedence

- `--out` takes precedence over `--format` when both specified
- `--query` is an alias for `--execute` (`-e`)
- Environment variable: `CQLITE_OUT` sets default output format
- `export` shows a determinate progress bar + ETA when `--limit N` is set (the only
  authoritative total), a spinner otherwise, and emits no progress/summary when
  `--quiet` or when stdout is piped/redirected (Issue #284).

### CLI modes (Issue #242)

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

## Python bindings

```bash
# Build and test
cd bindings/python && maturin develop --profile dev  # Development build (debug; overrides the release-unwind firewall pin for a fast dev loop)
cd bindings/python && maturin build --profile release-unwind  # Release wheel (panic-unwind firewall, issue #1440 — NOT --release, which is panic=abort)

# Run Python tests - fast tests only (default, Issue #331)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -v

# Run all Python tests including slow (CLI parity, performance)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets RUN_SLOW_TESTS=1 pytest bindings/python/tests -v

# Run only slow tests (CLI parity and performance)
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -m slow -v

# Exclude slow tests explicitly
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets pytest bindings/python/tests -m "not slow" -v
```

```bash
# Python usage example
python3 -c "
import cqlite
with cqlite.open('test-data/datasets/sstables', schema='test-data/schemas/basic-types.cql') as db:
    for row in db.execute('SELECT * FROM test_basic.simple_table LIMIT 5'):
        print(row.to_dict())
"

# Python Parquet export (Epic #682)
python3 -c "
import cqlite
with cqlite.open('test-data/datasets/sstables', schema='test-data/schemas/basic-types.cql') as db:
    rows = db.export_parquet('SELECT * FROM test_basic.simple_table', '/tmp/out.parquet',
                             row_group_size=10000, compression='snappy')
    print(f'Exported {rows} rows')
"
```

### Python bindings structure

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
├── tests/                 # pytest suite (counts churn — `ls tests/test_*.py`)
│   └── conftest.py        # Shared fixtures and path constants (Issue #330)
├── pyproject.toml         # Maturin build configuration
└── Cargo.toml             # Rust dependencies
```

### Python E2E test architecture (Issue #323)

Primary E2E tests (`bindings/python/tests/`):
- `test_parity.py`: Validates all 33 tables against JSONL golden files
  - `TestRowCountParity`, `TestValueParity`, `TestE2ESummary`
- `test_cli_parity.py`: Python vs CLI output equivalence

Known issues (tracked as XFail): none currently (issue #493, set element tombstones,
was the last and is closed).

## Node.js bindings

```bash
# Build and test (Issue #290, #296, #306)
cd bindings/node && npm install && npm run build  # Build native module
cd bindings/node && npm test                       # Run all tests (Jest)
cd bindings/node && npm run test:watch             # Watch mode for development
cd bindings/node && npm run test:coverage          # Run with coverage report
```

```bash
# Node.js usage example (Issue #296 - Phase 2 complete)
node -e "
const { Database } = require('@cqlite/node');
(async () => {
  const db = await Database.open('test-data/datasets/sstables', {
    schema: 'test-data/schemas/basic-types.cql'
  });
  const result = await db.executeNative('SELECT * FROM test_basic.simple_table LIMIT 5');
  console.log('Rows:', result.rowCount);
  for (const row of result.rows) {
    console.log(row.name);
  }
  await db.close();
})();
"

# Node.js Parquet export (Epic #682)
node -e "
const { Database } = require('@cqlite/node');
(async () => {
  const db = await Database.open('test-data/datasets/sstables', {
    schema: 'test-data/schemas/basic-types.cql'
  });
  const rows = await db.exportParquet(
    'SELECT * FROM test_basic.simple_table', '/tmp/out.parquet',
    { rowGroupSize: 10000, compression: 'snappy' });
  console.log('Exported', rows, 'rows');
  await db.close();
})();
"
```

### Node.js bindings structure

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
├── __test__/              # Jest suite (counts churn — `ls __test__/*.test.js`)
├── jest.config.js         # Jest configuration
├── Cargo.toml             # napi-rs dependencies
├── package.json           # npm package config (@cqlite/node)
└── index.d.ts             # Generated TypeScript definitions
```

**Status**: Phase 3 (Streaming) complete (Issue #305). Key APIs:
- `Database.open(dataDir, options?)` — open with optional schema
- `Database.execute(query)` — **deprecated** (removed next major; emits a `DeprecationWarning`); lossy legacy JSON (blob→base64 string, timestamp→ISO string, varint/decimal→bespoke strings) and slower. Use `executeNative()`
- `Database.executeNative(query)` — native JS types (BigInt, Date, Buffer, Set, Map)
- `Database.executeStreaming(query, config?)` — async iteration for large result sets
- `Database.getStats()` / `Database.close()`

For full Node.js API reference, TypeScript definitions, error codes, and streaming
details, see `bindings/node/lib/index.d.ts` and the issue backlog (#290, #296–#314).

## Python/Node thread safety and output parity

**Python thread safety** (Issue #311, #805, #815): `Arc<Database>` + `AtomicBool`; GIL
released during async ops; concurrent queries on the same database are safe without a
warm-up. Full scans no longer share mutable file state: #815 removed the old
`SSTableReader.scan_mutex` and gave every scan its own `ScanCursor` (independent
file handle + chunk index), so N concurrent full scans run in parallel rather than
serialized.

**Python/CLI parity** (Issue #319): Python uses native types (v0.13 mapping:
`timestamp`→`datetime`, `uuid`→`UUID`, `blob`→`bytes`, `time`→`int` ns since
midnight, `duration`→`cqlite.Duration` — see the
[v0.13 Migration Guide](v0.13-migration-guide.md)); CLI uses JSON
strings. Normalization required for comparison — see
`bindings/python/tests/test_cli_parity.py`.

## Write support (CLI)

```bash
# Build with write support (Issue #392)
cargo build --package cqlite-cli --features write-support

# Write a mutation (requires --writable and --write-dir)
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --mutation '{"table":{"keyspace":"test_basic","table":"simple_table"},"partition_key":[{"Uuid":[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]}],"clustering_key":[],"operations":[{"Write":{"column":"name","value":{"Text":"Test"}}}],"timestamp_micros":1704067200000000}'

# Flush memtable to SSTable
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --flush

# Issue #1253: a single combined invocation persists durably. `--execute` DML
# now runs BEFORE the flush within the same invocation, so the inserted row
# lands in Data.db (not just the WAL):
cargo run --package cqlite-cli --features write-support -- \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --execute "INSERT INTO test_basic.simple_table (id, name) VALUES (33333333-3333-3333-3333-333333333333, 'Carol')" \
  --flush

# Write subcommands
cargo run --package cqlite-cli --features write-support -- \
  maintenance --budget-ms 100 \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

cargo run --package cqlite-cli --features write-support -- \
  write-stats \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql

cargo run --package cqlite-cli --features write-support -- \
  export-sstable /tmp/export --keyspace my_ks --table my_tbl \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql
```

## Delta-export (CDC Parquet, Issue #705 / Epic #696 DS9)

Requires `--features delta-export`. Schema must be a bare `CREATE TABLE` statement
(no `CREATE KEYSPACE` / `USE` preamble).

```bash
cargo build --package cqlite-cli --features delta-export

# Export one SSTable generation as a delta-envelope Parquet file
cargo run --package cqlite-cli --features delta-export -- \
  delta-export test-data/datasets/sstables/test_basic/simple_table-<uuid> \
  --schema test-data/schemas/simple_table.cql \
  --out parquet \
  -o /tmp/delta.parquet

# With custom envelope prefix (to resolve __op/__ts column collisions)
cargo run --package cqlite-cli --features delta-export -- \
  delta-export test-data/datasets/sstables/test_basic/simple_table-<uuid> \
  --schema test-data/schemas/simple_table.cql \
  --out parquet \
  -o /tmp/delta.parquet \
  --envelope-prefix _cqlite_

# Run delta-export integration tests
env CQLITE_DATASETS_ROOT=$PWD/test-data/datasets \
  cargo test --package cqlite-cli --features delta-export --test delta_export_tests
```

## Feature-flag builds

```bash
# Minimal build (pure library, no query engine)
cargo build --package cqlite-core --no-default-features --features all-compression

# Build with CLI helpers for integration testing
cargo build --package cqlite-core --features cli-helpers

# Build/test core with the embeddable Parquet writer (Epic #682)
cargo build --package cqlite-core --features parquet
cargo test --package cqlite-core --features parquet
```

## Fuzzing (issue #1614)

Policy (nightly-only, out of the stable gate, workspace-excluded) is in `CLAUDE.md`. Run details:

- Five targets prove the parser never panics/hangs/OOMs on arbitrary bytes
  (returns `Ok` or `Err`): `fuzz_vint`, `fuzz_value_decode`, `fuzz_block_emit`,
  `fuzz_bti`, `fuzz_schema_parse`. They reach `cqlite-core` internals via the
  feature-gated `#[doc(hidden)] cqlite_core::fuzz_support` module (build with
  `--features fuzz`), which keeps the default public API unchanged.
- Run one target (needs `rustup toolchain install nightly` + `cargo install cargo-fuzz`):
  ```bash
  cd fuzz && cargo +nightly fuzz run fuzz_vint -- -max_total_time=45 -rss_limit_mb=2048 -timeout=25
  ```
  Or all targets: `fuzz/smoke.sh`. `fuzz_block_emit` fully exercises the
  block-emit path only when `CQLITE_DATASETS_ROOT` points at the test datasets
  (a real `test_basic/simple_table` fixture); otherwise it no-ops.
- CI: `.github/workflows/fuzz.yml` runs a bounded per-target PR smoke lane and a
  nightly long-run (both nightly + cargo-fuzz, isolated from the stable gate). A
  crash fails the job and uploads the reproducer artifact; crashes are filed as
  their own bug issues (not silently patched here).
