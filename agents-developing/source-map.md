---
title: Key Source Paths
description: Where parsers, writers, query engine, bindings, and test infrastructure live in the CQLite codebase.
sidebar:
  label: Key source paths
  order: 4
---

Use this page to locate code before reading. Grep in the right subtree rather than
scanning the whole workspace.

## Workspace layout

```
cqlite-core/     # Core library: SSTable parsing, query engine, write engine
cqlite-cli/      # CLI binary: TUI, REPL, one-shot mode, write subcommands
bindings/python/ # PyO3 bindings — M4 complete
bindings/node/   # napi-rs bindings — Phase 3 (Streaming) complete
test-data/       # Real Cassandra 5.0 SSTables + JSONL golden files
tools/           # sstabledump-validator, format-validator
scripts/         # agent-gate.sh, profile.sh, docs-site-check.sh
```

Planned: `bindings/wasm/` (WebAssembly, M6).

## cqlite-core

The core library. All SSTable format work lives here.

### SSTable reading

```
cqlite-core/src/storage/sstable/
├── reader/
│   └── parsing/
│       └── v5_compressed_legacy.rs  # Main V5 format parser (~2000 lines)
├── bti/                             # BTI (Big Table Index) trie format
└── row_cell_state_machine.rs        # OA format state-machine parser
```

`v5_compressed_legacy.rs` is the entry point for parsing `nb` and `oa` format
SSTables. It handles decompression, partition iteration, row flags, cell decoding,
and delta encoding against `EncodingStats` from `Statistics.db`.

`bti/` implements the BTI (trie-based) index format used by `da` SSTables. Parser
is incomplete — da tables are excluded from the default smoke run.

### SSTable writing

```
cqlite-core/src/storage/sstable/
└── writer/
    └── data_writer.rs   # Data.db writer
```

```
cqlite-core/src/storage/write_engine/
├── mod.rs              # WriteEngine public API
├── merge.rs            # K-way merge for compaction (M5.2)
├── merge_policy.rs     # STCS compaction policy (M5.2)
└── export.rs           # SSTable export (M5.2)
```

### Query and schema

```
cqlite-core/src/
├── parser/     # SSTable binary format parsing primitives (VInt, bytes, etc.)
├── cql/        # CQL text parsing: query strings → AST
├── query/      # Query engine (M2+): SELECT execution, filtering, projection
└── schema/     # Schema management: CQL DDL parsing, column type registry
```

`cql/` parses CQL query text into an AST. `query/` executes that AST against
SSTable data. `schema/` manages CREATE TABLE DDL and maps column names to CQL types.

### Key files to read first when debugging

| Problem area | Start here |
|---|---|
| Wrong row count | `row_cell_state_machine.rs` — partition/row boundaries |
| Wrong cell value | `v5_compressed_legacy.rs` — cell flag parsing + delta decode |
| Wrong type decode | `cqlite-core/src/schema/` — column type → decoder mapping |
| Query returns nothing | `cqlite-core/src/query/` — filter evaluation |
| Write corruption | `storage/sstable/writer/data_writer.rs` |
| Compaction issues | `storage/write_engine/merge.rs` |

## Python bindings (`bindings/python/`)

```
bindings/python/src/
├── lib.rs        # Module init — exports Database, QueryResult, Row, errors
├── database.rs   # Database class: open/close/execute/stats
├── result.rs     # QueryResult, Row, StreamingIterator
├── value.rs      # CQL → Python type conversions
├── error.rs      # cqlite_core::Error → PyErr mapping
├── config.rs     # StreamingConfig, presets
├── runtime.rs    # Tokio runtime lifecycle
├── prepared.rs   # PreparedStatement
└── stats.rs      # DatabaseStats
```

```
bindings/python/python/cqlite/
├── __init__.py    # Python-side wrapper
└── __init__.pyi   # Type stubs
```

Tests in `bindings/python/tests/` — 17 files, 360+ tests.

## Node.js bindings (`bindings/node/`)

```
bindings/node/src/
├── lib.rs         # napi-rs entry point, module exports
├── database.rs    # Database class, QueryResult, ColumnInfo
├── streaming.rs   # StreamingResult for async iteration
├── value.rs       # CQL → JavaScript type conversions
└── error.rs       # cqlite_core::Error → napi::Error
```

```
bindings/node/lib/
├── index.js         # Enhanced entry point with error wrapper
├── index.d.ts       # TypeScript definitions (hand-written, authoritative)
└── error-wrapper.js # JS error enhancement layer
```

Tests in `bindings/node/__test__/` — 13 files, 255 tests (Jest).

## Test infrastructure

```
test-data/
├── datasets/sstables/   # Extracted SSTable files + JSONL goldens
├── schemas/             # CQL schema files (basic-types.cql, collections.cql, ...)
└── scripts/
    ├── fetch-datasets.sh          # Download + verify + extract dataset
    ├── smoke-test-all-tables.sh   # Run CLI against every table, check exit codes
    ├── regenerate-datasets.sh     # Regenerate corpus with Docker + Cassandra
    ├── package_datasets.sh        # Tar + SHA256 for release
    └── publish_datasets.sh        # Push to GitHub releases

scripts/
├── agent-gate.sh     # THE gate — run before every PR
├── profile.sh        # Profiling: criterion, flamegraphs, heap (dhat)
└── docs-site-check.sh  # Docs CI parity check
```

```
cqlite-integration-tests/
└── tests/            # Seven CI-enforced integration test targets
    ├── chunked_data_reader_direct_test.rs
    ├── comprehensive_component_integration_tests.rs
    ├── fixture_specific_integration_tests.rs
    ├── golden_path_get_operations_tests.rs
    ├── golden_path_partition_lookup_tests.rs
    ├── golden_path_scan_operations_tests.rs
    └── golden_path_summary_index_integration_tests.rs
```

## Finding things with ripgrep

```bash
# Find where a CQL type is decoded
rg -n "CqlType::Duration" cqlite-core/src/

# Find partition key parsing
rg -n "parse_partition" cqlite-core/src/storage/

# Find where schema columns are resolved
rg -n "column_type" cqlite-core/src/schema/

# Find all feature-gated experimental code
rg -n "#\[cfg(feature = \"experimental\")\]" cqlite-core/src/

# Find all unwrap() calls (should be zero in library code)
rg -n "\.unwrap()" cqlite-core/src/ --glob '!*.rs.bk'
```

## Cassandra source reference

When the CQLite parser behaviour diverges from Cassandra's:

- Local copy: `~/local_projects/cassandra` (Cassandra 5.0 source)
- Remote: https://github.com/apache/cassandra/tree/cassandra-5.0.0
- Key file: `src/java/org/apache/cassandra/io/sstable/format/big/UnfilteredSerializer.java`
- Key file: `src/java/org/apache/cassandra/db/rows/Cell.java`
