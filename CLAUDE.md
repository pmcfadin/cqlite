# CLAUDE.md

Guidance for Claude Code when working with CQLite.

## Project Overview

CQLite is a Rust library for local Apache Cassandra SSTable access. It reads Cassandra 5.0 data files without cluster dependencies.

**Status**: v0.13.0 (Jul 2026) - the performance release. Core reading (M1), CLI (M2), Output Writers (M3), Python & Node.js Bindings (M4), and Write Support + STCS compaction (M5) are complete. v0.13.0 adds read-path constant-factor speedups (Epic E, C2), Node bindings throughput, byte-bounded result budgets (`Error::ResultTooLarge`), explicit `Database::refresh()`, and no-heuristics correctness fixes, on top of v0.12.0's byte-for-byte compaction parity vs Apache Cassandra, Arrow Flight + Trino connector, canonical BTI (`da`) write/read, CDC-style delta-export, and `WRITETIME()`/`TTL()` in `SELECT`. Next: M6 (WASM bindings), M7 (perf validation + v1.0).

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
  - CI toolchain policy: `docs/development/ci-toolchain-policy.md` (issue #1990) — every product-validation workflow honors `rust-toolchain.toml` (omit `toolchain:` for setup actions that read it; `dtolnay/rust-toolchain@1.88.0` explicit since it can't); exactly ONE advisory `future-rust-canary.yml` lane tracks latest `stable`; coverage tools install prebuilt (`taiki-e/install-action`), never `cargo install` from source.
  - Parity CI tier contracts: `docs/development/parity-ci-tiers.md` (what each Cassandra parity CI tier promises; gate-strength smoke/canonical-semantic/byte-for-byte) + `docs/development/parity-release-checklist.md` (gates public parity claims). Belongs alongside the gate-contract page on the `agents-developing/` site — mirror there when the site page lands (issue #1022).
    - The `exhaustive_regeneration` tier is backed by `.github/workflows/exhaustive-regeneration.yml` (weekly + `workflow_dispatch`, never on PRs; issue #1026), which regenerates the corpus and runs `cargo run -p cassandra-parity -- corpus-audit` (hard-fails on corpus/manifest or provenance drift).
    - `docs/reports/cassandra-test-parity.md` is a **committed derived artifact** rendered from `test-data/cassandra-parity-manifest.yml`. It can go stale on `main` via a **semantic merge race** (two manifest-changing PRs each regenerate correctly vs their base, but the squash-merge leaves the report rendered against a stale base) — no per-PR `--check` can catch this. Two safeguards (issue #1338): the SKIP-aware `parity-report` agent-gate component (`scripts/agent-gate.sh`) catches the single-PR forgot-to-regenerate case locally before push; the `parity-report-heal` push-to-`main` job in `.github/workflows/cassandra-parity.yml` self-heals the merge race by opening/updating a single regeneration PR from `auto/parity-report-regen` (never pushing to protected `main`). The heal job needs repo secret `PARITY_HEAL_TOKEN` (a PAT/App token with `contents`+`pull-requests` write) so the regen PR triggers CI — a PR opened by the default `GITHUB_TOKEN` gets no checks; absent the secret the job SKIPs with a `::notice::` (regenerate manually) instead of opening a check-less PR. See `docs/development/parity-ci-tiers.md`.
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
| `ci-cd-validation` | Tiered gate loop (lite iterate, full once), CI monitoring, merge-on-green |

**Delivery pipeline skills** (in `.claude/skills/`): `flow-groom` → `flow-activate` → `flow-implement` →
`flow-address` → `flow-finalize`, plus `flow-board` (claim board + next thing). See
`docs/development/pm-operating-loop.md`. (`start-epic`/`pm-status` are deprecated pointers → flow-*.)

## Available Subagents

Subagents in `.claude/agents/` for specialized tasks (pass an explicit `model` on spawn — the pinned
frontmatter model may be inaccessible):

| Agent | Purpose |
|-------|---------|
| `flow-lead` | Delivery lead/PM — drives the flow-* pipeline, sequences the specialists |
| `sstable-developer` | SSTable implementation, format debugging |
| `rust-reviewer` | Read-only Rust code review, quality enforcement |
| `test-validator` | Test execution, sstabledump parity, failure triage |
| `spec-auditor` | Intent audit (C) — impl vs OpenSpec/issue acceptance criteria |
| `coverage-reviewer` | Test-quality review (meaningful, not just present) |
| `compaction-parity-auditor` | Write/compaction byte-parity gap audit vs Cassandra |

## Essential Commands

```bash
# Canonical agent gate (issue #719) - THE pre-PR gate for agents.
# Runs fmt, clippy -D warnings, core tests (cli-helpers), integration tests,
# write-support tests, CLI tests, minimal-features build, and smoke, then
# emits a machine-checkable summary block. Paste that block verbatim when
# reporting validation; ad-hoc cargo runs do not count as "the gate passed".
#
# clippy is SCOPED per-package (issue #1844): it lints the whole workspace with
# -D warnings but does NOT compile the source-built DuckDB C++ amalgamation
# (cqlite-cli `duckdb-tests`) or the OpenTelemetry/OTLP stack
# (`observability`/`observability-testing`) — both were pure per-gate tax.
# parquet/arrow stay linted. Coverage of the excluded features moves to nightly:
# CQLITE_CLIPPY_FULL=1 runs the full `--workspace --all-targets --all-features`
# matrix, which .github/workflows/gate.yml (nightly deep-check) sets.
#
# Missing-fixtures fail-closed (issue #2078): the FULL gate FAILs CLOSED when the
# fetched validation corpus (test_basic/...) is absent, even though a fresh
# worktree's committed byte-parity reference *-Data.db files keep the raw Data.db
# count > 0 (previously a false PASS via SKIP). Opt-out:
# AGENT_GATE_ALLOW_MISSING_FIXTURES=1 restores the lenient SKIP and stamps a
# machine-checkable `missing-fixtures: OPT-OUT (...)` line in the SUMMARY, so an
# intentional opt-out is visible in the pasted artifact; absent the opt-out, a
# FAIL stamps `missing-fixtures: FAIL-CLOSED (#2078)` with the remedy: bash
# test-data/scripts/fetch-datasets.sh. --lite/--only are unchanged (lenient).
scripts/agent-gate.sh

# FAST ITERATION gate (issue #1821) - NOT the gate of record.
# Runs ONLY file-size + fmt + scoped workspace clippy (-D warnings, same #1844
# duckdb/otel-excluded scoping as the full gate) +
# blast-radius-scoped tests (the touched package's --lib + the diff's new --test
# targets, mapped from `git diff --name-only origin/main...HEAD`; defaults to
# `cqlite-core --lib` when no rust package is in the diff). ~1-5 min vs 12-25 min.
# Use it on EVERY fix round of the implement/roborev loop. It emits a DISTINCT
# "==== AGENT-GATE LITE SUMMARY ====" block (MODE: lite) that can NEVER be pasted
# as the full SUMMARY, and its recovery default is .agent-gate-lite-summary.txt.
# Lite NEVER replaces the full gate: run the full scripts/agent-gate.sh ONCE
# before merge and it must PASS - that SUMMARY is the only run that counts.
scripts/agent-gate.sh --lite

# TEST/DOCS-ONLY DELTA RE-CERTIFICATION (issue #1892) - NOT the gate of record.
# After a full-gate PASS at commit X, if the diff X..Y touches ONLY files the
# re-cert can EXECUTE (rust cargo test code, python binding tests, Node.js
# __test__/ tests run against an ALREADY-BUILT native module, scripts/tests/*.sh
# self-tests, and/or docs — issue #2081 moved node/shell from refused to
# executed), re-certify with --delta instead of forcing a whole new full gate.
# It FAILs CLOSED on anything else (src, scripts, workflows, Cargo.*, config,
# test-data, or an unbuilt node module — it NEVER builds with cargo and never
# passes vacuously) and forces a fresh full gate. Emits a DISTINCT
# "==== AGENT-GATE DELTA SUMMARY ====" block (MODE: delta) naming the gate of
# record (the full PASS at X) + the anchor run-id, so it can NEVER be pasted as
# a full SUMMARY. Record BOTH the anchor's full SUMMARY and this DELTA block in
# the PR. Standing backstop: the nightly gate.yml deep-check re-runs the FULL
# gate on main. Recovery default: .agent-gate-delta-summary.txt. Deep mechanics
# + the delta-executors: line: docs/development/gate-ops.md.
scripts/agent-gate.sh --delta <anchor-sha> --anchor-run-id <full-gate-run-id>
#   # or, to read the anchor run-id from the recorded full SUMMARY:
scripts/agent-gate.sh --delta <anchor-sha> --anchor-summary-file <path-to-full-SUMMARY>

# Capture the gate ROBUSTLY (issue #1175). The SUMMARY block is the only
# artifact that counts. The foreground redirect never buffers — prefer it:
bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
# Under tee / a pty / background capture, a leaked build-server or test daemon
# can hold the gate's stdout pipe open and truncate (even fully lose) a streamed
# SUMMARY even though the gate exited 0. RECOVERY (no need to parse stdout): pick
# the path in advance and read it — it is ALWAYS the complete block.
AGENT_GATE_SUMMARY_FILE=/tmp/gate-summary.txt bash scripts/agent-gate.sh > gate.log 2>&1 < /dev/null
cat /tmp/gate-summary.txt   # complete SUMMARY, even if gate.log truncated
# If you did not set AGENT_GATE_SUMMARY_FILE, the gate writes the same complete
# block to the documented default $PWD/.agent-gate-summary.txt (gitignored); cat
# that if your stream is missing the `==== END AGENT-GATE SUMMARY ====` marker.
# CONCURRENCY: that default is per-checkout; if you run multiple gates concurrently
# IN THE SAME CHECKOUT, give each a unique AGENT_GATE_SUMMARY_FILE or they clobber
# each other's recovery artifact (separate worktrees are already isolated).
# Fast self-test of the emission/recovery path:
bash scripts/tests/test_agent_gate_summary.sh
```

Every SUMMARY block (full and `--lite`) carries a machine-checkable `accelerators:` line (sccache/nextest/lane-parallelism state) — treat degradation shown there as actionable, not scrollback noise.

Deep gate operations (sccache tuning, concurrency-cap internals, disk hygiene, parallelism knobs, `--delta` mechanics): `docs/development/gate-ops.md`.

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
- `export` shows a determinate progress bar + ETA when `--limit N` is set (the only
  authoritative total), a spinner otherwise, and emits no progress/summary when
  `--quiet` or when stdout is piped/redirected (Issue #284).

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
- `Database.execute(query)` — **deprecated** (removed next major; emits a `DeprecationWarning`); lossy legacy JSON (blob→base64 string, timestamp→ISO string, varint/decimal→bespoke strings) and slower. Use `executeNative()`
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

**Python/CLI parity** (Issue #319): Python uses native types (v0.13 mapping:
`timestamp`→`datetime`, `uuid`→`UUID`, `blob`→`bytes`, `time`→`int` ns since
midnight, `duration`→`cqlite.Duration` — see the
[v0.13 Migration Guide](docs/development/v0.13-migration-guide.md)); CLI uses JSON
strings. Normalization required for comparison — see
`bindings/python/tests/test_cli_parity.py`.

## Development Standards

### No-Heuristics Mandate
Use authoritative metadata only — no type guessing. Schema-aware decoding when schema
present. Legacy heuristics behind opt-in `experimental` feature flag only.
See canonical doctrine: [no-heuristics mandate](https://pmcfadin.github.io/cqlite/agents-developing/no-heuristics/)

### Supported formats (version floor)
CQLite targets Cassandra 5.0 — `na`+/`nb` BIG and `oa`/`da` BTI are in scope; pre-`na`
(`ma`–`me`, Cassandra 3.x) is out of scope and SHALL NOT be introduced, supported, or
reviewed for correctness (this guidance is for reviewers incl. roborev too). The floor is
enforced in code: `BigVersionGates::from_version` rejects `< na` and `BtiVersionGates::from_version`
rejects non-`da`, both with `Error::UnsupportedVersion`; `SSTableReader::open` propagates that
error rather than falling back. Do not re-litigate pre-`na` "regressions."

### Write surface: CQLite writes UNCOMPRESSED SSTables (claim boundary, issue #1406)
The production write surface (flush + compaction via `SSTableWriter`) emits
**uncompressed** SSTables only and never emits a `CompressionInfo.db`. The
compressed-write building blocks (`CompressedDataWriter`, `CompressionInfoWriter`)
are built but **UNWIRED** — they exist solely to synthesize compressed fixtures for
the read/decompress path, and carry zero Cassandra-side byte-parity coverage for a
CQLite-emitted `CompressionInfo.db`. This is fail-closed in code: any attempt to
configure compressed production writing returns `Error::UnsupportedFormat`
(`SSTableWriter::with_compression` / `CompressionInfoWriter::guard_unsupported_production_write`).
Do NOT claim CQLite emits compressed SSTables (parity manifest records this as
`claim.blocked.compressed_sstable_writes`; the safe wording is
`claim.safe.uncompressed_sstable_writes`). Wiring compressed writes (posture a) is
tracked in issue #1406.

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

### Fuzzing (issue #1614)
- The parser fuzz harness lives at `fuzz/` — a **cargo-fuzz / libFuzzer** crate
  that is its own workspace and is **excluded from the main workspace**, so
  `scripts/agent-gate.sh` and every default `cargo build`/`clippy`/`test` neither
  compile nor depend on it. Fuzzing needs **nightly** Rust and is **out of the
  stable gate**.
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
cd bindings/python && maturin develop --profile dev
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
- **Tiered gate loop (issue #1821): iterate on `--lite`, run the FULL gate ONCE before merge.** The implement loop is `implement → lite (each fix round) → conditional internal rust-reviewer review → lite → FULL gate ONCE before merge → roborev → CI → merge`. Use `scripts/agent-gate.sh --lite` (fmt + file-size + workspace clippy + blast-radius-scoped tests, ~1-5 min) on every fix round; it is the fast iteration loop, **NOT the gate of record**. `--lite` NEVER replaces the full gate: run the full `scripts/agent-gate.sh` exactly ONCE before merge and it must PASS — its `==== AGENT-GATE SUMMARY ====` block is the only run that counts.
- **Test/docs-only delta re-certification (issue #1892): a post-gate polish round that touches ONLY executable tests/docs re-certifies with `--delta`, not a whole new full gate.** After a full-gate PASS at commit `X`, if the diff `X..Y` touches ONLY what the re-cert can EXECUTE — rust cargo test code (`.rs` under `tests/` dirs, `*_test(s).rs`), python binding tests (`bindings/python/tests/`, run by the #1893 python tier), Node.js binding tests (`bindings/node/__test__/*`, run against an ALREADY-BUILT native module), shell self-tests (`scripts/tests/*.sh`), and/or docs (`*.md` anywhere; TOP-LEVEL `docs/`, `website/`) — run `scripts/agent-gate.sh --delta X --anchor-run-id <X's full-gate run-id>` (or `--anchor-summary-file <path to X's full SUMMARY>`). It FAILs CLOSED — **anything** else in `X..Y` (src, scripts, workflows, `Cargo.*`, config, test-data, or an unbuilt node module, since issue #2081 moved node `__test__/` files and `scripts/tests/*.sh` from refused to executed) REFUSES the re-cert and forces a fresh full gate. On pass it runs file-size + fmt + the diff's changed test targets and emits a DISTINCT `==== AGENT-GATE DELTA SUMMARY ====` block (MODE: delta) carrying a `delta-executors:` line naming which executors ran. **Record BOTH artifacts in the PR:** the anchor's full SUMMARY (the gate of record) AND the `X..Y` DELTA block. The delta is NOT the gate of record and can never substitute for the full gate on a production change. The standing backstop is the nightly `.github/workflows/gate.yml` deep-check, which re-runs the FULL gate on `main` (owner condition, 2026-07-04). This closes the re-gate loophole where every roborev round on a Low test-robustness finding forced another 15–25 min full gate (e.g. #1853 burned 3 full gates and #1921 burned 2 on test/docs-only polish rounds). Deep `--delta` mechanics: `docs/development/gate-ops.md`.
- **Conditional review-first**: do an internal `rust-reviewer` pass BEFORE the first FULL gate when the diff changes a `pub` item, touches >1 call site of a changed symbol, or adds a new surface — catching those findings pre-full-gate avoids a wasted 12-25 min full-gate cycle per roborev round. Skip the review-first pass for mechanical/localized diffs.
- Clear roborev findings (run /roborev-fix) before handing an issue off.
- Stay within your assigned issue's scope; flag cross-cutting changes to the lead instead of editing another teammate's files.
- An issue is "done" only when tests pass, coverage meets threshold, roborev is clean, and both the spec-auditor and coverage-reviewer sign off.

### Pre-roborev self-check (common findings to pre-empt)
`roborev_findings` is the #1 recurring delivery cost (telemetry retro). Before reporting an implementation done, scan your diff for these recurring finding classes and fix them up front — every one avoided is a review round saved. Full guidance: https://pmcfadin.github.io/cqlite/agents-developing/roborev-findings/.
- **GitHub Actions command injection** — never interpolate `${{ inputs.* }}` / `${{ steps.*.outputs.* }}` directly into a `run:` shell (worst in a step holding secrets). Allowlist-validate the value fail-closed *before* any secret step, then pass it via a quoted env var (`-Pversion="$VAR"`), not inline `${{ }}`.
- **clippy `manual_range_contains`** — `x >= a && x <= b` fails under `-D warnings`. Write `(a..=b).contains(&x)`.
- **Integer overflow / saturation** — decoding into `i128`/fixed width and saturating (decimal unscaled values, scale math) loses data. Use `num_bigint::BigInt` (already a dep); bound the computation by comparing signs/adjusted-exponents *before* any large power-of-ten — never materialize `10^scale` with an unbounded exponent (DoS/OOM).
- **Float ordering vs Java** — Rust `total_cmp` ≠ Java `Float/Double.compare`: Rust puts negative NaN first, Java sorts NaN last; also signed-zero differs. When matching Cassandra, use an explicit comparator (NaN last, `-0.0 < +0.0`).
- **Wall-clock races in tests** — never assert a value sampled at one instant against a window captured at another (one-second boundary flakes). Capture the window to cover *all* sampled operations.
- **No-heuristics violations** — never infer type/behavior from byte patterns; use authoritative schema/`Statistics.db` metadata (see no-heuristics mandate).
- **Gitignored reference binaries / dirty-tree gate** — byte-parity tests silently SKIP in a clean checkout when `.db` references are gitignored. Force-add the tiny reference binaries (`git add -f`) and verify the test against a fresh `git worktree add --detach HEAD`, not the dirty tree.

### Spec-driven work (OpenSpec)
- OpenSpec is the front door for **design-driven** new work (bindings/M6, query-engine surface, CLI/REPL UX, perf/M7, process). **Oracle-driven** bug fixes (SSTable parsing, compaction/tombstone parity, type decode) stay as a GitHub issue + a pinned parity test — no OpenSpec change.
- Merge flow for a design-driven change: `apply → gate (correctness) → C (intent audit) → roborev (code) → merge → archive`. The intent audit **C** is the `spec-auditor` subagent anchored to `openspec/changes/<name>/specs/**`; it runs only after `scripts/agent-gate.sh` is green. **B** (optional) reuses `roborev-design-review-branch` with the change's artifacts as criteria — escalate when C reports `partial`, the change is high-stakes, or it touches doctrine.
- An OpenSpec change is "done" only when the gate passes, **C reports PASS** (every requirement `satisfied` with a public-surface test as evidence; an `unmet`/uncovered/unjustified-`partial` requirement blocks merge), and roborev is clean — then `openspec archive`.
- superpowers vs OpenSpec: superpowers are *techniques* (brainstorming, TDD, receiving-code-review); OpenSpec is the *artifact system + lifecycle*. They nest — `brainstorming` is the method inside `explore`; the OpenSpec proposal/design/tasks ARE the plan (no parallel `plan.md`). See https://pmcfadin.github.io/cqlite/agents-developing/spec-driven-audit/.

### Delivery pipeline (flow-lead)
- The delivery lead is the **`flow-lead`** manager agent (the repo's default agent; `claude --agent flow-lead`). It orchestrates — it spawns and sequences the specialists (`sstable-developer`, `rust-reviewer`, `spec-auditor`/C, `test-validator`, `coverage-reviewer`) + roborev + `agent-gate.sh` — and does not write production code itself.
- Pipeline verbs (skills): `flow-groom` → `flow-activate` (Seam 1: spec approval) → `flow-implement` (gate → C → roborev → PR) → `flow-address` → `flow-finalize` (archive + cleanup + close); `flow-board` surfaces the single next thing. Full doctrine: https://pmcfadin.github.io/cqlite/agents-developing/delivery-pipeline/.
- **Tiered gate inside `flow-implement` (issue #1821).** The implement/fix loop runs `scripts/agent-gate.sh --lite` (fmt + file-size + workspace clippy + blast-radius-scoped tests, ~1-5 min) on EACH fix round, does a **conditional internal `rust-reviewer` review-first** pass before the first FULL gate (when the diff changes a `pub` item, touches >1 call site of a changed symbol, or adds a new surface; skip for mechanical/localized diffs), then runs the FULL `scripts/agent-gate.sh` exactly ONCE before merge — its `==== AGENT-GATE SUMMARY ====` block is the only run that counts. Loop: `implement → lite (each round) → conditional review-first → lite → FULL gate ONCE → roborev → CI → merge`. **`--lite` NEVER replaces the full gate.** Rationale + measurement plan: `process_improvements.md`. **Post-gate polish rounds (issue #1892):** once the FULL gate has PASSed at `X`, a roborev/address round whose diff `X..Y` is test/docs-ONLY re-certifies with `scripts/agent-gate.sh --delta X --anchor-run-id <run-id>` (fail-closed on any production change) rather than repeating the full gate — record BOTH the anchor full SUMMARY and the DELTA block in the PR; the nightly `gate.yml` deep-check is the standing backstop.
- **1:1:1:1**: one issue ↔ one worktree/branch `issue-<N>-<slug>` ↔ one OpenSpec change `<slug>` ↔ one PR. The GitHub Project board `Status` field is the source of truth (one `P0`–`P3` per issue); `status:*` labels are decorative, NOT a dispatch source (Path A, #1886).
- **Coordination & concurrency (Path A, #1886):** the GitHub Project board `Status` field is the **sole dispatch authority**; `status:*` labels are decorative/non-authoritative and MUST NOT be used to select or claim work. A session claims by **pushing the `issue-<N>-<slug>` branch to origin** (the cross-machine lock — assignee `@me` is identical for one user on two machines) + assignee + `Status=In Progress`, then re-reads. Newly created issues auto-land at `Status=Backlog` (Project built-in "item added → Backlog"). Default model is **one lead → subagents**; multiple independent sessions MUST use the claim protocol; never run N bare leads without it. **One worker per machine (#1930):** exactly one flow-lead worker runs per machine as the sole machine-load authority — it fans out to subagents but **serializes the full `agent-gate.sh` (concurrency = 1)** (the #1825 cap stops SIGKILL, not tail-latency flakes) and pre-claims by checking for **any** `issue-<N>-*` branch (any slug, not just its own). Cross-*machine* concurrency stays coordinated by the origin branch lock. `flow-board` reaps abandoned `In Progress` claims. **If the board is unreachable (`project` scope/auth), STOP and fix auth — do NOT dispatch from labels** (empty Ready = no work ready, not a cue to dredge labels). See the delivery-pipeline doc.
- When spawning a subagent, pass an explicit accessible model (e.g. opus) — the pinned frontmatter model is not always accessible.
- **Self-improvement loop (telemetry + retro).** The pipeline measures itself: `flow-finalize` stamps one record per completed issue into the append-only ledger `docs/reports/delivery-telemetry.jsonl` (schema `docs/reports/delivery-telemetry.schema.json`) via `scripts/delivery-telemetry.py record` — authoritative data only (GitHub timestamps → cycle time + phase durations; run-observed counters for claim collisions, rebases, gate pass/fail, roborev findings, rework; a counter not observed is an error, never a fabricated `0`). On a cadence the manager runs `delivery-telemetry.py retro` to rank recorded failures by a documented weighted tally (deterministic, not inferred) and file a deduped `flow-meta` improvement issue. The `delivery-telemetry` agent-gate component (SKIP-aware) covers the tool. Doctrine: `docs/development/pm-operating-loop.md`.

## Product-manager behavior (lead)
- The lead acts as product manager: track epics and issues, prioritize, and keep work moving.
- **Autonomy — auto-merge on green (default):** workers (and the lead) **merge their own PR autonomously** the moment the quality bar is met — `agent-gate.sh` PASS + **C** PASS (design-driven) + roborev clean — via `gh pr merge --squash --delete-branch` then `flow-finalize`. Do NOT wait for the owner to merge. The owner's spec approval (Seam 1) is the only standing human gate; merge is NOT a human gate. Escalate to the owner and **hold the merge** ONLY for: a genuine design-call roborev finding, a scope/product question, an unmet/uncovered requirement, or work outside the issue — and obey any manager `HOLD: merge after #N` order (block until #N lands). (This supersedes the prior "pre-authorized merge-on-green / merge is the owner's Seam 2" model.)
- Autonomous GitHub writes still permitted within these limits: post comments; add/remove status labels; assign or reassign issues. Closing a fully-done non-epic issue with a merged linked PR (with a closing comment) is allowed; merging follows the model above.
- Never close an epic, never change an issue's scope or title, and never make a product decision (ambiguous scope, conflicting requirements, tradeoffs) without me — collect those under a "NEEDS YOU" list and surface them.
- Make every write traceable with a short comment so I can review or reverse it later.
