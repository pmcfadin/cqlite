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

FD/RSS resource-leak soak (long-running open/scan/drop loop): see
`docs/development/soak-resource-leak.md`.

Measuring against a **multi-GB** corpus (cold/warm scans, large-I/O work): generate it with
`test-data/scripts/gen-perf-corpus-3068.sh` (BIG/`nb`) or
`test-data/scripts/gen-perf-corpus-bti.sh` (BTI/`da`, below) and run every measurement through
`test-data/scripts/perf-run-contained.sh` — an *uncontained* cold read of an 8 GiB mmap'd `Data.db`
hard-hung a swapless host for 75 minutes with no OOM kill. See
`docs/development/perf-corpus-and-containment.md`.

## BTI (`da`) perf corpus — `gen-perf-corpus-bti.sh` (issue #3234)

Every committed `da-*-bti-*` fixture is a *correctness* fixture (largest: `test_da/wide_table`, a
28 KB `Data.db`), so BTI read-path work needs a generated corpus. Two independent reasons:

- a warm scan of the committed fixtures finishes in microseconds — ~6 orders of magnitude short of a
  ≥10 s profiling window;
- `MADV_RANDOM` is applied only at `file_size >= 8 MiB`, so below that the point-read and scan
  mappings are **the same mapping** and a read-plane A/B is structurally zero, not merely noisy.

```bash
# End-to-end pipeline validation (~2.5 min: 60 s boot + 20 s restart + ~10 s load + golden).
# Defaults the keyspace to perf_bti_smoke so it can never clobber a production corpus.
bash test-data/scripts/gen-perf-corpus-bti.sh --smoke --out /data/corpus-3234-bti

# Production corpus: ~2.0 GiB over 27 SSTables (default --rows 13200000 --chunk-rows 500000).
bash test-data/scripts/gen-perf-corpus-bti.sh --out /data/corpus-3234-bti
bash test-data/scripts/gen-perf-corpus-bti.sh --rows 33000000     # ~5 GiB

bash test-data/scripts/gen-perf-corpus-bti.sh --validate-only     # flags only; no container, no writes
bash test-data/scripts/gen-perf-corpus-bti.sh --verify-only        # re-assert an existing corpus, offline
bash test-data/scripts/gen-perf-corpus-bti.sh --help              # every flag + its env var
```

The final line printed is the `export CQLITE_DATASETS_ROOT=<abs>` to use.

**Economics** (measured by the **production** commissioning run on a fleet worker box, 27 chunks →
1.995 GiB): **162.3 B/row on disk** at `--payload-bytes 160` with LZ4 `chunk_length_in_kb=16`, and
**~68k rows/s** end-to-end including CSV generation (13.2 M rows loaded in 194 s). Phase breakdown of
that 7.3-minute run: ~3.2 min of container boot + BTI restart + yaml verification, ~3.2 min of load,
~40 s of asserts + the one `sstabledump` golden. So ~5 GiB ≈ 33 M rows ≈ **~12 min**.
`--chunk-rows 500000` gives ~77.4 MiB per `Data.db` (measured largest 81,151,240 B), an order of
magnitude over the 8 MiB floor; the last chunk is the `--rows` remainder and is smaller (32.4 MiB
here) but still over the floor.

**`pk` is a CQL `int`, so the chunk count has a hard ceiling.** Chunk *N*'s partition keys start at
`N * PK_STRIDE`, and the largest key an `int` column can hold is 2,147,483,647. The generator's
`plan_fits_int32` refuses an over-ceiling `(chunks, chunk-rows)` plan at `--validate-only` time —
*before* any container starts — because the failure mode otherwise costs a partial multi-GB load: at
the original 1e9 stride, chunk 3 of 27 began at 3,000,000,000 and `cqlsh COPY` rejected **every** row
of it (`'i' format requires -2147483648 <= number <= 2147483647`), four minutes and three SSTables
in, while the 2-chunk `--smoke` run never reached chunk 3. The stride is now 1e6, admitting 2147
chunks.

**The two mandatory `cassandra.yaml` settings.** A stock Cassandra 5.0 node emits **`nb` (BIG)**,
because it ships `storage_compatibility_mode: CASSANDRA_4`. Both of these are required, and both
must be in place *before* the table is created (the script applies them, restarts, and then
`grep`-verifies each one):

```yaml
storage_compatibility_mode: NONE     # live in the shipped yaml (~line 2249)
sstable:                             # COMMENTED OUT in the shipped yaml (~line 1142)
  selected_format: bti
```

A miss on **either** silently produces `nb` with no error at all — which is why the yaml greps and
the emitted descriptors are hard failures, not warnings. The fail-closed asserts (all of them
re-runnable offline via `--verify-only`, and pinned by `scripts/tests/test_gen_perf_corpus_bti.sh`
with a negative control each) are: `da-*-bti-*` descriptors only and **no `nb-*`**; ≥1 `Data.db`
> 8 MiB; every `Rows.db` non-empty; each TOC lists `Partitions.db`/`Rows.db` and **not** the
BIG-only `Index.db`/`Summary.db`; rows loaded == `Statistics.db` `totalRows` == `sstabledump` rows
for each dumped generation; and the manifest writer's plan-vs-`Statistics.db` cross-check on **both**
the row count and the partition count (an unreadable `Partition Size` histogram is an error, never a
fabricated 0).

The `sed` that flips those two settings depends on the shipped file's exact comment markers and
two-space indentation, so it lives in one snippet-emitting function used by two callers: the
container path, and `--yaml-flip-check FILE`, a self-test hook that runs the **same text** against
`scripts/tests/fixtures/cassandra-5.0.2-cassandra.yaml.excerpt` (a committed verbatim excerpt of the
image's yaml). Two more hermetic hooks exist for the same reason: `--prune-dry-run`
(+ `PRUNE_KEEP=<basename>`) enumerates the multi-GB dirs a run would `rm -rf` and deletes nothing,
and `DOCKER=scripts/tests/fixtures/stub-docker-cassandra-bti.py` stands in for the container so the
whole pipeline — including both row-count cross-checks and the manifest writer's happy path — runs in
a test with no Cassandra. `--smoke` overrides only the DEFAULTS: an explicit `--rows`/`--chunk-rows`
(or `ROWS`/`CHUNK_ROWS`) survives it.

**Manifest identity** — `test-data/perf-corpus-bti-manifest.json` (committed; mirrors
`perf-corpus-3068-manifest.json`). The corpus itself is multi-GB and **not** committed
(`.gitignore`: `*.db`), so what is reproducible matters — and the two halves are different:

- **The seed reproduces the ROW SET.** The row driver (`gen-perf-corpus-bti-rows.py`) seeds chunk *N*
  with `"<seed>:<N>"`, so every value, the partition count, the rows-per-partition distribution and
  the chunk→SSTable split are a pure function of `(seed, chunk-index)` — not merely the row *count*.
  That is the deliberate divergence from the `#3068` BIG sibling, whose `cassandra-stress` profile
  cannot be reproduced from anything a manifest can record.
- **The seed does NOT reproduce the `Data.db` bytes.** Cassandra stamps a wall-clock write timestamp
  on every row, serialized as an unsigned VInt *delta* from the `Statistics.db` `min_timestamp`
  baseline (Ch.5 §"temporal deltas"), so a later run shifts some deltas across a VInt width boundary
  and even the file length changes — **measured**: two same-seed smoke runs produced 19,474,015 B and
  19,474,397 B. The per-SSTable **sha256** is therefore an *instance identity* (prove two measurements
  ran on the same bytes; catch silent corruption or an accidental replacement), **not** a regeneration
  check. A sha mismatch after regenerating is expected, not a defect.

**Timing a sustained warm scan over it** — `cqlite-core/examples/bti_perf_scan.rs` (committed, so the
measurement is reproducible). It drives `Database::execute_streaming` to exhaustion, not the Flight
`do_get` plane: per issue #3233 BTI is denied the Flight bypass arm, and a criterion bench would spend
minutes of warm-up + samples to report a distribution where a profile needs one sustained window.

```bash
cargo build --release -p cqlite-core --example bti_perf_scan --features cli-helpers
bash test-data/scripts/perf-run-contained.sh --mem 12G --swap 0 -- \
  ./target/release/examples/bti_perf_scan \
    --corpus /data/corpus-3234-bti-full --keyspace perf_bti --table wide_multiclustering \
    --warm-passes 1 --min-seconds 10
```

**WHICH PLANE the number describes — this is not "the BTI read path" in general.**
`execute_streaming` is a library entry point, not one fixed storage route, and the route is a function
of the corpus:

- **27 generations + a resolved schema** (the production corpus) satisfies
  `readers.len() > 1 && schema.is_some()` (`storage/sstable/mod.rs:2141-2148`, `write-support`) and
  routes into `generation_merge::stream_generations_for_read`. Its `KWayMerger` drives one sequential
  producer per generation, and each producer **re-opens its SSTable with `use_mmap = false` /
  `DiskAccessMode::Buffered`** (`storage/write_engine/merge/producer_iter.rs:364-388`), walking
  `Data.db` via `stream_all_partitions_for_compaction`. So the 125.6 s below measures the
  **compaction-style BTI `Data.db` stitch + decode plus the k-way merge, over buffered I/O** — **not**
  the mmap plane and **not** `run_scan_stream`'s BTI trie branch.
- **One generation (or an unresolved schema)** falls through to the per-reader `scan_stream`, where a
  BTI reader takes the trie branch. That is a **separate measurement** with a different memory
  profile, and it is the one to run for the mmap/trie plane.

The harness therefore **prints the route beside the number** — `generations:`, `schema_resolved:`,
`access_path:` (the per-query probe reset at `select_executor/mod.rs:525`) and a `storage_route:` line
naming the branch it took — so a throughput figure can never again be quoted without its plane.

**Containment is not optional, and the streaming channel does not bound RSS on every route.** On the
multi-generation merge route the consumer drops rows as they arrive, so the window is bounded; but a
**single-generation** (or schema-less) invocation on a multi-GB BTI corpus takes the trie branch,
which **pre-materializes the whole reconciled table** before streaming (issue #1577 — the exact
condition `scan_stream_materializes` reports `true` on, `storage/sstable/mod.rs:2045-2054`). That is a
multi-GB allocation and precisely the #3068 livelock shape. Always run under
`test-data/scripts/perf-run-contained.sh`.

It is fail-closed on every way a dataset-dependent measurement can lie. The **row-count assert is ON
by default**: with no flag the harness reads the authoritative count from
`<corpus>/manifest-bti-3234.json`, else the committed
`test-data/perf-corpus-bti-manifest.json` (`rows_per_partition.rows`, recorded *observed, not
requested*, and cross-checked against `row_count_cross_check`), and an absent / unparseable /
other-table manifest exits `8` rather than degrading to "assert off". That is the guard that catches a
**silently truncated** scan: `execute_streaming` surfaces producer *errors* as a terminal `Err`, but a
producer *panic* drops its `JoinHandle` and closes the channel (the #3124 class), which the consumer
sees as a clean end-of-stream — a short row count is the only signal there is. Exit codes: `2` usage
(incl. a non-finite or non-positive `--min-seconds`, which `f64::parse` accepts), `3` corpus
missing/open failed, `4` zero rows, `5` row-count mismatch (any pass, warming ones included), `6`
window under the floor — printing the row count that *would* reach it — `7` a scan that started then
failed mid-stream, `8` no authoritative row count. Both asserts have a loud opt-**out**
(`--no-expect-rows`, `--no-min-seconds`) that stamps `*** UNGUARDED: … ***` on the `RESULT:` line, and
`--warm-passes 0` labels its output `COLD` rather than passing a cold scan off as the AC3 number.
Every one of those codes is observed firing by `scripts/tests/test_bti_perf_scan.sh` (37 hermetic
cases against the committed 10 KiB `test_da` BTI fixture — no perf corpus, seconds to run), which the
gate's `tooling-tests` component runs.

**MEASURED on the 1.995 GiB / 13.2 M-row production corpus** (fleet worker box, warm page cache, one
discarded warming pass, the multi-generation merge route above): **125.6 s** wall clock, 13,200,000
rows, **105,073 rows/s** — 12.5× the ≥10 s window issue #3234 AC3 asks for, so the window survives
even a 10× read-path speed-up. Open cost is negligible (27 SSTables discovered in 0.033 s). The two
passes agreed to within 1.2 % (127.1 s vs 125.6 s), which is what confirms the measured pass was
steady-state warm rather than fault-bound. The mmap/trie plane is **not** covered by this number.

**Re-confirmed after the harness was hardened** (same box, same corpus, row-count assert ON and read
from `/data/corpus-3234-bti-full/manifest-bti-3234.json`): **127.163 s**, 13,200,000 rows verified,
**103,804 rows/s**, warm pass 126.231 s (0.7 % apart), `generations: 27`, `schema_resolved: true`,
`access_path: fallback_full_scan (partition_key_not_fully_constrained)` — the honest CQL-level label
for an unrestricted `SELECT *` — and `storage_route: generation_merge::stream_generations_for_read`.
The access path is the *query*-level signal; `storage_route` is the plane, and both are printed on
every run.

Every number in the manifest is read back from the written bytes (`sstablemetadata` on
`Statistics.db`, the `CompressionInfo.db` header, each `TOC.txt`) and **nothing is inherited from a
previous manifest**. A `mode` field marks whether a manifest describes a `smoke` validation run or
the `production` corpus.

**This is a parity oracle, not just a throughput fixture.** Every byte is **Cassandra-written**, so
the `sstabledump -l` JSONL goldens emitted beside the corpus can back parity work. Per issue #3042 a
CQLite-written round-trip fixture cannot: both halves make the identical framing mistake, so the
round-trip closes while real Cassandra-written data reads wrong. Goldens run ~2× the `Data.db` size,
so only a bounded subset is dumped (`--dump-generations`, default 1) and they live beside the
(gitignored) corpus — the `git add -f` convention applies to the small `test_da` correctness
goldens, never to these. **Measured**: the one golden for a 500k-row generation is 160,752,721 B
(153.3 MiB), **1.98×** its `Data.db`, with 711 partition lines and exactly 500,000 row objects —
matching that generation's `Statistics.db` `totalRows` and `partition_count`. At 153 MiB it is far too
large to commit; regenerate it on demand instead (a *committable* BTI golden means a dedicated small
table, not a slice of this corpus).

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
```

**CI/release profile parity (issue #2653):** both `python-ci.yml` (smoke, build-only-wheels, test
jobs) and `python-release.yml` (build-wheels job) build the wheel with `--profile release-unwind`,
so CI exercises the exact panic = "unwind" firewall build that PyPI ships and a panic-strategy
regression reds a PR instead of surfacing only at release. When changing the build profile in one
workflow, change it in the other to keep the matrix in parity.

```bash
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

## Runtime tuning knobs (env)

Parsed once per process; unset = shipped default (behavior unchanged).

| Env var | Default | Meaning |
|---------|---------|---------|
| `CQLITE_READ_PATH` | `auto` | Force the read path (`auto`/`point`/`compact`), issue #1918. |
| `CQLITE_FLIGHT_MERGE_PATH` | unset (`auto`) | Force the Flight `do_get` ROW route's arm (issue #3058): `merge` never takes the single-source fast path (the field kill switch — restores the pre-#3058 k-way merge for every request with no redeploy); `bypass` requests the fast path; unset/`auto`/anything unrecognized = automatic. `bypass` NEVER overrides a correctness precondition — a request with ≥2 post-prune sources, a non-empty `dropped_columns`, a STATIC column, an aggregation, or a reader the single-generation walk cannot serve still merges. Read ONCE per request. Which arm actually ran is observable via `cqlite_core::storage::read_path_probe` (merger-construction / reconcile-entry / cell-metadata-map counters) — that is how `cqlite-flight/tests/issue_3058_forced_path_differential.rs` proves the two arms return byte-identical rows over the same bytes at a pinned `now`. |
| `CQLITE_EGRESS_ROW_BUDGET` | `2048` | Adaptive merge egress budget (issue #2765): per-channel `sync_channel` capacity = `clamp(budget / concurrent_merges, min_cap, 256)`. Raise to allow more prefetch buffering per merge under concurrency; lower to cap aggregate memory. Missing/unparseable/zero → default. **Residual K-linear dimension**: the budget divides by merge COUNT only, not per-merge fanout K, so a single wide merge still buffers up to `K × 256` entries (~60MB at K=100) invariant to concurrency — intended ("solo merge unchanged for any K"); the high-K envelope is covered by the #2895 loadgen sweep. |
| `CQLITE_EGRESS_MIN_CAP` | `8` | Forward-progress floor for the above (clamped to `[1, 256]`; budget forced `≥ min_cap`). The floor engages only at very high concurrency (`budget / min_cap` ≈ 256 concurrent merges at defaults). **Inert-throttle cases** (per-channel cap constant, never shrinks with concurrency): setting this `≥ 256` (floor meets the 256 ceiling), OR a budget `< 2 × min_cap` (degenerate range, cap pinned at `min_cap`). The DEFAULTS (2048/8) do NOT disable the throttle — it engages above 8 concurrent merges. A one-time `tracing::warn!` fires on exactly these two inert cases. Fresh loadgen validation tracked in #2895. |

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

## Publish dispatches (armed by default? NO — issue #2639)

The publishing workflows are guarded so a bare `workflow_dispatch` cannot publish
to Maven Central or mint/move a release tag from an arbitrary ref. Both guards are
enforced by `scripts/ci/validate-workflows.rb` (they fail the workflow-lint if
removed) and documented in `docs/ci/ci-tier-policy.md` (Release tier).

```bash
# Trino connector: bare dispatch is a DRY RUN (publishToMavenLocal only, no
# Central upload, no secrets) — dry_run DEFAULTS TO TRUE.
gh workflow run trino-publish.yml -f version=0.15.0

# A real Maven Central release requires dry_run=false explicitly:
gh workflow run trino-publish.yml -f version=0.15.0 -f dry_run=false

# flight-image: a manual `version` dispatch REFUSES unless refs/tags/v$version
# already resolves to the commit the run builds (github.sha). Push the release
# tag first, then dispatch with that tag's ref selected:
git push origin v0.15.0
gh workflow run flight-image.yml --ref v0.15.0 -f version=0.15.0

# For a one-off, NON-release image (no vX.Y.Z / latest tags), use image_tag:
gh workflow run flight-image.yml -f image_tag=dev-preview
```

A `v*` tag push (`git push origin v0.15.0`) publishes for real automatically on
both lanes — the guards only constrain manual dispatches.
