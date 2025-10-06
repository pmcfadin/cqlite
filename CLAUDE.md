# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CQLite is a high-performance Rust library providing local Apache Cassandra SSTable access. It enables reading and writing Cassandra data files without cluster dependencies, built for performance and safety with planned bindings for Python, NodeJS, and WASM.

**Project Status**: Early Development (M1 Milestone - Core Reading Library)

## Essential Commands

### Building and Testing

```bash
# Build the project (workspace)
cargo build

# Build with release optimizations
cargo build --release

# Run all tests (requires test data)
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets cargo test --package cqlite-core

# Run tests with timeout (recommended)
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets timeout 60s cargo test --package cqlite-core

# Run single test by name
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets cargo test --package cqlite-core test_name_here

# Run specific test file
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets cargo test --package cqlite-core --test file_name

# Run with verbose output
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets cargo test --package cqlite-core -- --nocapture

# Run quiet mode (one character per test)
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets cargo test --package cqlite-core --quiet
```

### Code Quality

```bash
# Run clippy (linter)
cargo clippy --workspace --all-targets --all-features

# Run clippy with warnings as errors (CI mode)
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features

# Format code
cargo fmt

# Check formatting without making changes
cargo fmt --check

# Run specific clippy check on core package
cargo clippy --package cqlite-core
```

### Coverage and Benchmarking

```bash
# Generate code coverage report
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets cargo tarpaulin --packages cqlite-core --out Stdout --exclude-files "tests/*" --exclude-files "src/bin/*" --timeout 120

# Run benchmarks
cargo bench --package cqlite-core
```

### CLI Tool

```bash
# Run CLI (from workspace root)
cargo run --bin cqlite -- <command>

# Run CLI with release optimizations
cargo run --release --bin cqlite -- <command>
```

## Architecture Overview

### Workspace Structure

CQLite uses a Cargo workspace with multiple crates:

- **cqlite-core**: Core database engine (SSTable parsing, storage, query execution)
- **cqlite-cli**: Command-line interface tool
- **cqlite-ffi**: Foreign Function Interface bindings for C/C++ integration
- **cqlite-wasm**: WebAssembly bindings for browser deployment
- **tests**: Integration test suite
- **examples**: Example usage code
- **tools/**: Specialized validation and testing tools
  - `sstabledump-validator`: Validates parsing against Apache Cassandra's sstabledump
  - `format-validator`: Format checking and validation utilities

### Core Architecture (cqlite-core)

The core library is organized into these key modules:

#### Storage Layer (`src/storage/`)
- **SSTable Management** (`storage/sstable/`): Handles SSTable format parsing, reading, and component discovery
  - `reader.rs`: Primary SSTable reading interface
  - `directory.rs`: SSTable file discovery and organization
  - `compression.rs`: LZ4, Snappy, Deflate, Zstd decompression
  - `index_reader.rs`: Partition index parsing for fast lookups
  - `header_spec.rs`: SSTable header format specifications
  - `bti/`: Big Table Index (BTI) support for Cassandra 5.0+
- **MemTable** (`storage/memtable.rs`): In-memory write buffer
- **Write-Ahead Log** (`storage/wal.rs`): Durability guarantees
- **Compaction** (`storage/compaction.rs`): Background SSTable merging
- **Manifest** (`storage/manifest.rs`): Metadata tracking

#### Parser Layer (`src/parser/`)
- **Dual Backend Support**: Switchable between nom and ANTLR4 parsers
- **AST Definitions** (`ast.rs`): Abstract Syntax Trees for CQL statements
- **Visitor Pattern** (`visitor.rs`): AST traversal and transformation
- **Binary Parsing** (`binary.rs`): SSTable binary format parsing
- **Statistics** (`statistics.rs`, `enhanced_statistics_parser.rs`): SSTable metadata parsing
- **Complex Types** (`complex_types.rs`, `optimized_complex_types.rs`): Collections, UDTs, tuples

#### Query Engine (`src/query/`)
- **QueryEngine** (`engine.rs`): Main query execution coordinator
- **QueryPlanner** (`planner.rs`): Query optimization and planning
- **QueryExecutor** (`executor.rs`): Statement execution
- **PreparedQuery** (`prepared.rs`): Prepared statement support
- **SELECT Components** (feature-gated with `state_machine`):
  - `select_parser.rs`: Dedicated SELECT statement parser
  - `select_executor.rs`: SELECT execution engine
  - `select_optimizer.rs`: Query optimization

#### Schema Management (`src/schema/`)
- **SchemaManager**: Schema discovery, validation, and evolution
- **Parser Integration**: Schema parsing from CQL DDL and SSTable metadata

#### Supporting Infrastructure
- **Error Handling** (`error.rs`): Comprehensive error types using `thiserror`
- **Type System** (`types.rs`): CQL type mappings and conversions
- **Memory Management** (`memory/`): Memory allocation and tracking
- **Platform Abstraction** (`platform/`): OS-specific implementations
- **Validation** (`validation/`): Data and format validation
- **Testing Infrastructure** (`testing/`, `docker/`): Test utilities and Docker integration

### Test Data Requirements

**CRITICAL**: Tests require real Cassandra 5.0 SSTable files located at:
```
test-data/datasets/sstables/
```

The test data includes:
- `test_basic/simple_table`: Basic single-column tables
- `test_collections/collection_table`: Collection types (lists, sets, maps)
- `test_timeseries/`: Time-series data patterns
- `test_wide_rows/wide_partition_table`: Wide partition testing

Reference files in JSONL format are also required for validation (`.jsonl` files alongside SSTables).

### Feature Flags

#### Active Features (M1)
- `default = ["all-compression", "metrics", "experimental"]`
- `all-compression`: Includes lz4, snappy, deflate, zstd
- `experimental`: SSTable writing and M1 functionality
- `legacy-heuristics`: **NOT in default** - Opt-in for backward compatibility with pre-5.0 formats

#### Disabled Features (M2+)
- `antlr`: ANTLR4 parser integration
- `state_machine`: Advanced query state orchestration (gates entire `query` module at module level)
  - When disabled, no query code is compiled into M1 builds
  - Query engine, planner, executor, prepared statements, and caching are all excluded
  - Use SSTableReader and storage layer directly for M1 reading operations
- `events`: Validation event recording
- `tombstones`: Tombstone and garbage collection logic
- `benchmarks`: Performance benchmark suite

#### Test Infrastructure Features
- `test-infrastructure`: Enhanced TestContext framework
- `docker-integration`: Docker-based integration testing

#### Query Module Gating (Issue #108)
The entire `query` module is gated behind the `state_machine` feature, which is **DISABLED by default** in M1:
- Query orchestration, planning, caching, and prepared statements are M2+ features
- The `Database` struct's query-related methods (`execute()`, `prepare()`, `explain()`) require `state_machine` feature
- For M1 basic SSTable reading, use the storage layer directly (see M1 API Examples below)
- **Note**: DashMap and parking_lot dependencies remain required as they're used by the storage layer

To enable query functionality (M2+ development):
```bash
cargo build --features state_machine
```

### M1 API Usage Examples

For M1 milestone (basic SSTable reading), use the storage layer directly without query engine:

#### Opening and Reading an SSTable
```rust
use cqlite_core::storage::sstable::reader::SSTableReader;

// Open an SSTable
let reader = SSTableReader::open("path/to/Data.db").await?;

// Read all entries
let entries = reader.get_all_entries().await?;

for (table_id, row_key, value) in entries {
    println!("Table: {}, Key: {:?}, Value: {:?}", table_id, row_key, value);
}
```

#### Using Index-Based Partition Lookups
```rust
use cqlite_core::storage::sstable::index_reader::IndexReader;

let index = IndexReader::new("path/to/Index.db").await?;
let partition_offset = index.lookup_partition(&partition_key)?;
// Use offset to read specific partition from Data.db
```

#### Working with SSTable Directory
```rust
use cqlite_core::storage::sstable::directory::SSTableDirectory;

let dir = SSTableDirectory::discover("/path/to/sstable/dir").await?;
for component in dir.components() {
    println!("Component: {:?}", component);
}
```

**Note**: The high-level `Database` API with query execution requires the `state_machine` feature and is not available in default M1 builds.

## Development Guidelines

### Milestone Context

Currently on **M1: Core Reading Library** focusing on:
- SSTable format parsing (Cassandra 5.0+ with BTI)
- Basic read operations
- Index-based partition lookups
- Real test data validation (no mocks in integration tests)
- **No-heuristics mandate** (Issue #28): Modern Cassandra 5 paths use authoritative metadata only
  - Header/format/compression detection uses structured metadata, not guessing
  - Schema-aware decoding enforced when schema present
  - Blob fallbacks removed from modern paths
  - Legacy heuristics gated behind opt-in `legacy-heuristics` feature (NOT in CI)

### Code Quality Standards

1. **Clippy Configuration**: Workspace uses M1-balanced clippy settings
   - `correctness` and `suspicious`: deny
   - `perf`, `style`, `complexity`: warn
   - Pedantic checks allowed during M1 for velocity

2. **Test Requirements**:
   - All integration tests must use real SSTable data
   - Unit tests may use synthetic data when feature `unit-tests-only` is enabled
   - Test data must be validated against `sstabledump` output

3. **Error Handling**:
   - Use `thiserror` for library errors
   - Use `anyhow` for application-level errors in binaries
   - Propagate errors with `?` operator, avoid unwrap/expect in library code

4. **Performance**:
   - Memory usage target: <128MB for large SSTables
   - Parse speed target: 1GB files in <10 seconds
   - Query latency target: Sub-millisecond partition lookups

### Running CI Locally

The M1 CI pipeline validates:
1. Compilation without warnings (`RUSTFLAGS="-D warnings"`)
2. Clippy correctness and suspicious checks
3. All tests pass with real Cassandra 5.0 data
4. Code coverage tracking (90% minimum target)

Reproduce CI locally:
```bash
# Full CI validation sequence
env RUSTFLAGS="-D warnings" cargo clippy --workspace --all-targets --all-features
cargo fmt --check
env CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets cargo test --package cqlite-core --quiet
```

### Common Development Patterns

#### Adding a New SSTable Component Parser
1. Define component in `storage/sstable/mod.rs`
2. Implement parser in dedicated module (e.g., `storage/sstable/new_component.rs`)
3. Add component discovery in `storage/sstable/directory.rs`
4. Create integration test using real SSTable data
5. Validate against `sstabledump` output

#### Adding a New CQL Type
1. Define type in `types.rs`
2. Add parser support in `parser/types.rs`
3. Implement serialization/deserialization
4. Add conversion logic in type system
5. Create test with real SSTable containing that type

#### Working with Binary Formats
- Use `nom` combinators for binary parsing
- Reference `header_spec.rs` for format specifications
- Validate byte-level parsing with hex dumps from real files
- Document bit layouts and field structures

## Troubleshooting

### Test Failures

**Missing Test Data**:
```
Error: CQLITE_DATASETS_ROOT environment variable not set
```
Solution: Set `CQLITE_DATASETS_ROOT=/Users/patrick/local_projects/cqlite/test-data/datasets`

**Timeout Issues**:
```
test result: FAILED. <X> tests exceeded timeout
```
Solution: Use `timeout` wrapper or increase test timeout in CI

**Compilation Errors with Features**:
Ensure you're not enabling M2+ features (antlr, state_machine, events) during M1 development.

### Performance Issues

- Use `cargo flamegraph` for profiling
- Check memory usage with `cargo instruments` on macOS
- Run performance benchmarks with `cargo bench`
- Validate against performance baseline in `target/criterion/`

## Key Files and Locations

- **Main Library Entry**: `cqlite-core/src/lib.rs`
- **SSTable Reading**: `cqlite-core/src/storage/sstable/reader.rs`
- **Parser Entry**: `cqlite-core/src/parser/mod.rs`
- **Error Definitions**: `cqlite-core/src/error.rs`
- **Test Utilities**: `cqlite-core/tests/common/`
- **CI Configuration**: `.github/workflows/m1-ci.yml`
- **Coverage Config**: `Cargo.toml` → `[package.metadata.coverage]`

## Resources

- **Cassandra 5.0 Format Documentation**: https://opensource.docs.scylladb.com
- **CQL Grammar**: https://github.com/pmcfadin/cassandra-antlr4-grammar
- **Project Issues**: https://github.com/pmcfadin/cqlite/issues
- **Validation Artifacts**: `cqlite-core/validation_artifacts/sstabledump/`