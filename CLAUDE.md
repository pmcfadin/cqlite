# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CQLite is a high-performance Rust library providing local Apache Cassandra SSTable access. It enables reading and writing Cassandra data files without cluster dependencies, built for performance and safety with planned bindings for Python, NodeJS, and WASM.

**Project Status**: Active Development (M2+ Milestone - Query Engine & CQL Support)

**Note**: M1 (Core Reading Library) is complete. M2+ adds query execution, CQL parsing, and high-level database APIs.

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

#### Active Features (M2+ Default Build)
- `default = ["all-compression", "metrics", "experimental", "state_machine"]`
- `all-compression`: Includes lz4, snappy, deflate, zstd compression support
- `metrics`: Performance monitoring and telemetry
- `experimental`: SSTable writing and experimental functionality
- `state_machine`: **NOW ENABLED BY DEFAULT** - Query engine, planner, executor, prepared statements, and caching
- `legacy-heuristics`: **NOT in default** - Opt-in for backward compatibility with pre-5.0 formats

#### Optional Features (M2+)
- `antlr`: ANTLR4 parser integration (alternative to nom)
- `events`: Validation event recording
- `tombstones`: Tombstone and garbage collection logic
- `benchmarks`: Performance benchmark suite

#### Test Infrastructure Features
- `test-infrastructure`: Enhanced TestContext framework
- `docker-integration`: Docker-based integration testing

#### Query Module Status (Issue #108)
The entire `query` module is gated behind the `state_machine` feature, which is **ENABLED by default** in M2+ builds:
- Query orchestration, planning, caching, and prepared statements are core functionality
- The `Database` struct's query-related methods (`execute()`, `prepare()`, `explain()`) are available by default
- M2+ standard development includes full query engine capabilities
- **Historical Note**: During M1 milestone, `state_machine` was disabled by default

To build minimal/M1-compatible binaries (storage layer only, no query engine):
```bash
cargo build --no-default-features --features all-compression,metrics
```

### Low-Level Storage API Examples

**M2+ Development Note**: These examples show low-level storage layer APIs for minimal builds or advanced use cases. Standard M2+ development should use the high-level `Database` API with query execution (see Query Engine section above).

**For Minimal Builds**: When building with `--no-default-features` (M1 compatibility mode), use the storage layer directly without query engine.

**Important**: All SSTable APIs require Platform and Config initialization:

```rust
use std::sync::Arc;
use std::path::Path;
use cqlite_core::{Config, Platform};

// Initialize required components (reuse across multiple operations)
let config = Config::default();
let platform = Arc::new(Platform::new(&config).await?);
```

#### Opening and Reading an SSTable
```rust
use cqlite_core::storage::sstable::reader::SSTableReader;
use std::sync::Arc;
use std::path::Path;
use cqlite_core::{Config, Platform};

// Initialize Platform and Config
let config = Config::default();
let platform = Arc::new(Platform::new(&config).await?);

// Open an SSTable (requires path as &Path, &Config, and Arc<Platform>)
let path = Path::new("path/to/Data.db");
let reader = SSTableReader::open(path, &config, platform.clone()).await?;

// Read all entries
let entries = reader.get_all_entries().await?;

for (table_id, row_key, value) in entries {
    println!("Table: {}, Key: {:?}, Value: {:?}", table_id, row_key, value);
}
```

#### Using Index-Based Partition Lookups
```rust
use cqlite_core::storage::sstable::index_reader::IndexReader;
use std::sync::Arc;
use std::path::Path;
use cqlite_core::{Config, Platform};

// Initialize Platform and Config
let config = Config::default();
let platform = Arc::new(Platform::new(&config).await?);

// Open Index.db (method is 'open', not 'new')
let index_path = Path::new("path/to/Index.db");
let index = IndexReader::open(index_path, platform.clone()).await?;

// Look up partition offset
let partition_offset = index.lookup_partition(&partition_key)?;
// Use offset to read specific partition from Data.db
```

#### Working with SSTable Directory
```rust
use cqlite_core::storage::sstable::directory::SSTableDirectory;
use std::path::Path;

// Scan directory (method is 'scan', not 'discover', and is NOT async)
let dir = SSTableDirectory::scan(Path::new("/path/to/sstable/dir"))?;

// Iterate over generations (not 'components')
for generation in &dir.generations {
    println!("Generation: {}", generation.generation);
    for (component_type, component_path) in &generation.components {
        println!("  Component: {:?} at {:?}", component_type, component_path);
    }
}

// Access latest generation
if let Some(latest) = dir.latest_generation() {
    println!("Latest generation: {}", latest.generation);
}
```

**Note**: The high-level `Database` API with query execution is available by default in M2+ builds. Use these low-level APIs only for minimal builds (`--no-default-features`) or when direct SSTable access is required.

## Development Guidelines

### Milestone Context

Currently on **M2+: Query Engine & CQL Support** building on completed M1 foundation:

**M1 Complete** (Core Reading Library):
- SSTable format parsing (Cassandra 5.0+ with BTI)
- Basic read operations
- Index-based partition lookups
- Real test data validation (no mocks in integration tests)
- **No-heuristics mandate** (Issue #28): Modern Cassandra 5 paths use authoritative metadata only
  - Header/format/compression detection uses structured metadata, not guessing
  - Schema-aware decoding enforced when schema present
  - Blob fallbacks removed from modern paths
  - Legacy heuristics gated behind opt-in `legacy-heuristics` feature (NOT in CI)

**M2+ In Progress** (Query Engine):
- CQL SELECT statement parsing and execution
- Query planning and optimization
- Prepared statement support
- High-level `Database` API with query methods
- Multi-partition query execution

### Code Quality Standards

1. **Clippy Configuration**: Workspace uses balanced clippy settings for velocity
   - `correctness` and `suspicious`: deny
   - `perf`, `style`, `complexity`: warn
   - Pedantic checks allowed during rapid development

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

The CI pipeline validates:
1. Compilation without warnings (`RUSTFLAGS="-D warnings"`)
2. Clippy correctness and suspicious checks
3. All tests pass with real Cassandra 5.0 data (including query engine tests)
4. Code coverage tracking (90% minimum target)

Reproduce CI locally:
```bash
# Full CI validation sequence (includes query engine)
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
The default build includes the query engine (`state_machine` feature). For minimal builds without query support, use `--no-default-features`.

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
- **CI Configuration**: `.github/workflows/` (M1 and M2+ pipelines)
- **Coverage Config**: `Cargo.toml` → `[package.metadata.coverage]`

## Resources

- **Cassandra 5.0 Format Documentation**: https://opensource.docs.scylladb.com
- **CQL Grammar**: https://github.com/pmcfadin/cassandra-antlr4-grammar
- **Project Issues**: https://github.com/pmcfadin/cqlite/issues
- **Validation Artifacts**: `cqlite-core/validation_artifacts/sstabledump/`