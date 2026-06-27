# CLAUDE.md

Guidance for Claude Code when working with CQLite.

## Project Overview

CQLite is a Rust library for local Apache Cassandra SSTable access. It reads Cassandra 5.0 data files without cluster dependencies.

**Status**: v0.12.0 (Jun 2026) - Core reading (M1), CLI (M2), Output Writers (M3), Python & Node.js Bindings (M4), and Write Support + STCS compaction (M5) are complete. v0.12.0 adds byte-for-byte compaction parity vs Apache Cassandra, an Arrow Flight + Trino connector, canonical BTI (`da`) write/read, CDC-style delta-export, and `WRITETIME()`/`TTL()` in `SELECT`. Next: M6 (WASM bindings), M7 (perf validation + v1.0).

## Documentation

### Primary Reference
**SSTable Format**: `docs/sstables-definitive-guide/README.md` - Single source of truth

Key chapters:
- Ch.5: Data.db Format (rows, flags, V5CompressedLegacy)
- Ch.6: Index.db/Summary.db (partition lookups)
- Ch.17: BTI Formats (trie indexes)
- Appendix B: Encoding Cheat Sheet (VInt, flags)
- Appendix F: Known Limitations (what doesn't work yet)

### Agent Developer Docs (canonical — site is source of truth)

Full contributor doctrine is published at `https://pmcfadin.github.io/cqlite/agents-developing/`:
- [Gate contract](https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/) — `scripts/agent-gate.sh`, summary-block format
  - Parity CI tier contracts: `docs/development/parity-ci-tiers.md` (what each Cassandra parity CI tier promises; gate-strength smoke/canonical-semantic/byte-for-byte) + `docs/development/parity-release-checklist.md` (gates public parity claims). Belongs alongside the gate-contract page on the `agents-developing/` site — mirror there when the site page lands (issue #1022).
- [No-heuristics mandate](https://pmcfadin.github.io/cqlite/agents-developing/no-heuristics/) — authoritative metadata only (issue #28)
- [Test data](https://pmcfadin.github.io/cqlite/agents-developing/test-data/) — fetching, dataset pins, CQLITE_DATASETS_ROOT
- [Key source paths](https://pmcfadin.github.io/cqlite/agents-developing/source-map/) — parsers, writers, query engine, bindings
- [sstabledump validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/) — JSONL goldens, parity tests, smoke
- [Format debugging workflow](https://pmcfadin.github.io/cqlite/agents-developing/format-debugging/) — hex dumps, guide chapters, Appendix F

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
| `rust-skills` | General idiomatic Rust (265 rules: ownership, errors, async, API design, anti-patterns); invoke with `/rust-skills` |

## Available Subagents

Subagents in `.claude/agents/` for specialized tasks:

| Agent | Model | Purpose |
|-------|-------|---------|
| `sstable-developer` | sonnet | SSTable implementation, format debugging |
| `rust-reviewer` | sonnet | Code review, quality enforcement |
| `test-validator` | haiku | Test execution, sstabledump parity |

## Essential Commands

```bash
# Canonical agent gate (issue #719) - THE pre-PR gate for agents.
# Runs fmt, clippy -D warnings, core tests (cli-helpers), integration tests,
# write-support tests, CLI tests, minimal-features build, and smoke, then
# emits a machine-checkable summary block. Paste that block verbatim when
# reporting validation; ad-hoc cargo runs do not count as "the gate passed".
scripts/agent-gate.sh

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

# Profiling loop (see docs/profiling.md)
./scripts/profile.sh baseline        # save criterion baseline
./scripts/profile.sh flame           # CPU flamegraphs (pprof, works in containers)
./scripts/profile.sh heap            # dhat heap profile vs <128MB budget
./scripts/profile.sh bench && ./scripts/profile.sh compare   # re-measure vs baseline
./scripts/profile.sh report          # ranked bottleneck report + history.jsonl ledger

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

# Python Parquet export (Epic #682)
python3 -c "
import cqlite
with cqlite.open('test-data/datasets/sstables', schema='test-data/schemas/basic-types.cql') as db:
    rows = db.export_parquet('SELECT * FROM test_basic.simple_table', '/tmp/out.parquet',
                             row_group_size=10000, compression='snappy')
    print(f'Exported {rows} rows')
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

# CLI with write support (Issue #392)
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

# Delta-export (CDC Parquet, Issue #705 / Epic #696 DS9) - requires --features delta-export
# Schema must be a bare CREATE TABLE statement (no CREATE KEYSPACE / USE preamble).
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

For the full source map (parsers, writers, query engine, bindings layout), see
[Key source paths](https://pmcfadin.github.io/cqlite/agents-developing/source-map/).

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

### Node.js Bindings Structure

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
├── __test__/              # 13 test files, 255 tests (Jest)
├── jest.config.js         # Jest configuration
├── Cargo.toml             # napi-rs dependencies
├── package.json           # npm package config (@cqlite/node)
└── index.d.ts             # Generated TypeScript definitions
```

**Status**: Phase 3 (Streaming) complete (Issue #305). Key APIs:
- `Database.open(dataDir, options?)` — open with optional schema
- `Database.execute(query)` — deprecated; use `executeNative()` for human-readable types
- `Database.executeNative(query)` — native JS types (BigInt, Date, Buffer, Set, Map)
- `Database.executeStreaming(query, config?)` — async iteration for large result sets
- `Database.getStats()` / `Database.close()`

For full Node.js API reference, TypeScript definitions, error codes, and streaming
details, see `bindings/node/lib/index.d.ts` and the issue backlog (#290, #296–#314).

### Python/Node.js Thread Safety and Output Parity

**Python thread safety** (Issue #311, #805, #815): `Arc<Database>` + `AtomicBool`; GIL
released during async ops; concurrent queries on the same database are safe without a
warm-up. Full scans no longer share mutable file state: #815 removed the old
`SSTableReader.scan_mutex` and gave every scan its own `ScanCursor` (independent
file handle + chunk index), so N concurrent full scans run in parallel rather than
serialized.

**Python/CLI parity** (Issue #319): Python uses native types (datetime, UUID, bytes);
CLI uses JSON strings. Normalization required for comparison — see
`bindings/python/tests/test_cli_parity.py`.

## Development Standards

### No-Heuristics Mandate
Use authoritative metadata only — no type guessing. Schema-aware decoding when schema
present. Legacy heuristics behind opt-in `experimental` feature flag only.
See canonical doctrine: [no-heuristics mandate](https://pmcfadin.github.io/cqlite/agents-developing/no-heuristics/)

### Code Quality
- `RUSTFLAGS="-D warnings"` must pass
- No `unwrap()`/`expect()` in library code
- Use `thiserror` for errors
- Memory target: <128MB for large files

### File Size (Campsite Rule)
Keep files small so they are cheap and accurate to read/edit — agentic context cost
scales directly with file size (a file must be read before it can be edited).
- **Targets** (total lines, inline tests included): source `~800`, test files `~1500`.
- The agent gate runs a **file-size ratchet** (`file-size` component): it lists changed
  `.rs` files over threshold (advisory) and **FAILs if your change makes an
  over-threshold file larger** (or pushes one over).
- **When you touch an over-threshold file**, split it by responsibility as part of your
  work — see epic #1116 (source split doctrine: `foo.rs` → `foo/mod.rs` + concern
  submodules with re-exports; tests follow their code) and #1135 (test files: extract
  shared fixtures, split by scenario).
- You may always edit a big file; you just cannot silently grow it. If a split is
  genuinely out of scope or too risky for the current change, re-run with
  `CQLITE_ALLOW_FILE_GROWTH=1` to acknowledge and leave a note linking #1116/#1135.

### Testing
- Integration tests use real SSTable data only
- Validate against sstabledump output
- JSONL reference files for parity checking
- See [sstabledump validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/)

### Python E2E Test Architecture (Issue #323)

**Primary E2E Tests** (`bindings/python/tests/`):
- `test_parity.py`: Validates all 33 tables against JSONL golden files
  - `TestRowCountParity`, `TestValueParity`, `TestE2ESummary`
- `test_cli_parity.py`: Python vs CLI output equivalence

**Known Issues** (tracked as XFail): none as of Dec 2025. Issue #493 (set element
tombstones) is out-of-scope for v0.9.1.

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

Without Data.db files, query tests will pass but return 0 rows. See
[Test data](https://pmcfadin.github.io/cqlite/agents-developing/test-data/) for
dataset pins and cache-key rationale.

## Feature Flags

Default (cqlite-core): `all-compression`, `state_machine`

| Feature | Description | In Defaults? |
|---------|-------------|--------------|
| `all-compression` | LZ4, Snappy, Deflate, Zstd support | Yes |
| `state_machine` | Query engine and discovery | Yes |
| `cli-helpers` | CLI-specific ingestion/REPL API (Issue #249) | No |
| `parquet` | Embeddable Parquet export writer (Epic #682) | No |
| `delta-scan` | CDC delta-record streaming API (Epic #696) | No |
| `delta-export` | CLI `delta-export` subcommand (Issue #705) | No |
| `metrics` | Performance metrics collection | No |
| `experimental` | Experimental features | No |

```bash
# Minimal build (pure library, no query engine)
cargo build --package cqlite-core --no-default-features --features all-compression

# Build with CLI helpers for integration testing
cargo build --package cqlite-core --features cli-helpers

# Build/test core with the embeddable Parquet writer (Epic #682)
cargo build --package cqlite-core --features parquet
cargo test --package cqlite-core --features parquet
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
- **Agent developer docs**: https://pmcfadin.github.io/cqlite/agents-developing/
- **Project Issues**: https://github.com/pmcfadin/cqlite/issues
- **Cassandra Source (local)**: `~/local_projects/cassandra` - Full Cassandra 5.0 codebase
- **Cassandra Source (remote)**: https://github.com/apache/cassandra/tree/cassandra-5.0.0

## Agent-team conventions
- Implementers commit after each meaningful unit of work so roborev reviews land while context is fresh.
- Clear roborev findings (run /roborev-fix) before handing an issue off.
- Stay within your assigned issue's scope; flag cross-cutting changes to the lead instead of editing another teammate's files.
- An issue is "done" only when tests pass, coverage meets threshold, roborev is clean, and both the spec-auditor and coverage-reviewer sign off.

### Spec-driven work (OpenSpec)
- OpenSpec is the front door for **design-driven** new work (bindings/M6, query-engine surface, CLI/REPL UX, perf/M7, process). **Oracle-driven** bug fixes (SSTable parsing, compaction/tombstone parity, type decode) stay as a GitHub issue + a pinned parity test — no OpenSpec change.
- Merge flow for a design-driven change: `apply → gate (correctness) → C (intent audit) → roborev (code) → merge → archive`. The intent audit **C** is the `spec-auditor` subagent anchored to `openspec/changes/<name>/specs/**`; it runs only after `scripts/agent-gate.sh` is green. **B** (optional) reuses `roborev-design-review-branch` with the change's artifacts as criteria — escalate when C reports `partial`, the change is high-stakes, or it touches doctrine.
- An OpenSpec change is "done" only when the gate passes, **C reports PASS** (every requirement `satisfied` with a public-surface test as evidence; an `unmet`/uncovered/unjustified-`partial` requirement blocks merge), and roborev is clean — then `openspec archive`.
- superpowers vs OpenSpec: superpowers are *techniques* (brainstorming, TDD, receiving-code-review); OpenSpec is the *artifact system + lifecycle*. They nest — `brainstorming` is the method inside `explore`; the OpenSpec proposal/design/tasks ARE the plan (no parallel `plan.md`). See https://pmcfadin.github.io/cqlite/agents-developing/spec-driven-audit/.

### Delivery pipeline (flow-lead)
- The delivery lead is the **`flow-lead`** manager agent (the repo's default agent; `claude --agent flow-lead`). It orchestrates — it spawns and sequences the specialists (`sstable-developer`, `rust-reviewer`, `spec-auditor`/C, `test-validator`, `coverage-reviewer`) + roborev + `agent-gate.sh` — and does not write production code itself.
- Pipeline verbs (skills): `flow-groom` → `flow-activate` (Seam 1: spec approval) → `flow-implement` (gate → C → roborev → PR) → `flow-address` → `flow-finalize` (archive + cleanup + close); `flow-board` surfaces the single next thing. Full doctrine: https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/.
- **1:1:1:1**: one issue ↔ one worktree/branch `issue-<N>-<slug>` ↔ one OpenSpec change `<slug>` ↔ one PR. Backlog = issues + labels (one `P0`–`P3`, one `status:*`).
- **Coordination & concurrency:** a GitHub Project board (`Status` field) is the shared claim board; a session claims by **pushing the `issue-<N>-<slug>` branch to origin** (the cross-machine lock — assignee `@me` is identical for one user on two machines) + assignee + `Status=In Progress`, then re-reads. Default model is **one lead → subagents**; multiple independent sessions MUST use the claim protocol; never run N bare leads without it. `flow-board` reaps abandoned `In Progress` claims. Falls back to `status:*` labels if the `project` scope/board is absent. See the delivery-pipeline doc.
- When spawning a subagent, pass an explicit accessible model (e.g. opus) — the pinned frontmatter model is not always accessible.

## Product-manager behavior (lead)
- The lead acts as product manager: track epics and issues, prioritize, and keep work moving.
- **Autonomy — pre-authorized merge-on-green** (supersedes the looser merge/close reading below): by DEFAULT the lead opens a PR but does NOT merge or close it (merge is the owner's Seam 2). The lead MAY squash-merge + `flow-finalize` ONLY a set the owner has EXPLICITLY pre-authorized ("merge #X,#Y on green"), and only when `agent-gate.sh` PASS + C PASS + roborev clean all hold.
- Autonomous GitHub writes still permitted within these limits: post comments; add/remove status labels; assign or reassign issues. Closing a fully-done non-epic issue with a merged linked PR (with a closing comment) is allowed; merging follows the model above.
- Never close an epic, never change an issue's scope or title, and never make a product decision (ambiguous scope, conflicting requirements, tradeoffs) without me — collect those under a "NEEDS YOU" list and surface them.
- Make every write traceable with a short comment so I can review or reverse it later.
