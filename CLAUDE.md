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
cargo run --bin cqlite -- <command>
```

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

Default: `all-compression`, `metrics`, `experimental`, `state_machine`

```bash
# Minimal build (no query engine)
cargo build --no-default-features --features all-compression,metrics
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
