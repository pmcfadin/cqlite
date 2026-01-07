# CLAUDE.md

Guidance for Claude Code when working with CQLite.

## Project Overview

CQLite is a Rust library for local Apache Cassandra SSTable access. It reads Cassandra 5.0 data files without cluster dependencies.

**Status**: M2+ (Query Engine) - M1 (Core Reading) is complete.

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
cqlite-ffi/      # C/C++ bindings
cqlite-wasm/     # WebAssembly bindings
test-data/       # Real Cassandra 5.0 SSTables for testing
tools/           # sstabledump-validator, format-validator
```

### Key Source Paths

```
cqlite-core/src/
├── storage/sstable/           # SSTable parsing
│   ├── reader/parsing/        # Format parsers
│   │   └── v5_compressed_legacy.rs  # Main V5 parser
│   ├── bti/                   # BTI index support
│   └── row_cell_state_machine.rs    # OA format parser
├── parser/                    # CQL parsing
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

## Test Data

Location: `test-data/datasets/sstables/`

| Keyspace | Tables | Purpose |
|----------|--------|---------|
| test_basic | 8 | Simple types |
| test_collections | 8 | Lists, sets, maps |
| test_timeseries | 9 | Time-series patterns |
| test_wide_rows | 8 | Wide partitions |

**Current pass rate**: ~27% (see Appendix F for details)

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

**Clippy failures**: Run with `RUSTFLAGS="-D warnings"` to match CI

**Parsing issues**: Check `docs/sstables-definitive-guide/chapters/appendix-f-known-limitations.md`

## Resources

- **Definitive Guide**: `docs/sstables-definitive-guide/`
- **Project Issues**: https://github.com/pmcfadin/cqlite/issues
- **Cassandra Source (local)**: `~/local_projects/cassandra` - Full Cassandra 5.0 codebase
- **Cassandra Source (remote)**: https://github.com/apache/cassandra/tree/cassandra-5.0.0
