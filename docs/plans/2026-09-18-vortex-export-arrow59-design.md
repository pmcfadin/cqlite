# Vortex export + the arrow 59 rev — design (2026-09-18)

Owner-validated design from a brainstorming session on 2026-09-18. Two issues fall out of it;
both belong to the toolbox (0.18) release. This document is the record of the decisions, not the
spec: issue B's OpenSpec change carries the requirements.

## Decisions (owner)

| # | Decision | Ruling |
|---|----------|--------|
| 1 | Role of Vortex | **Export target** (`.vortex` files), sibling of the Parquet export. Reading Vortex is a later slice. |
| 2 | Row semantics | **Reconciled rows**, the `SELECT` view — same semantics the Parquet writer emits, so the two exports agree. |
| 3 | Two Arrow generations | **Rev the workspace to arrow 59**, not an isolated crate with an IPC bridge. |
| 4 | DataFusion spike (`cqlite-flight`, `datafusion-spike`, `=44.0.0`) | **Port to DataFusion 55.1**, not removed. |
| 5 | Published Rust floor (`rust-version = "1.85"`) | **Raise to 1.95** with the rev. |
| 6 | Vortex version policy | **Pin exact** (`=0.86.1`), bump deliberately per release (lead recommendation, not contradicted). |

## Why arrow 59 and not 60

arrow 59.2 is the version Vortex 0.86.1 (`arrow-array ^59.2`, `parquet ^59.2`), DataFusion 55.1
(`arrow ^59.2`, `parquet ^59.2`) and arrow-flight 59.3 all share. Targeting 59 leaves the
workspace with exactly ONE arrow tree, so the `dep-duplicates` advisory stays flat. arrow 60.0.0
(2026-09-15) has no consumer here yet. Vortex's on-disk format is stable from 0.36; its crate API
is not — hence decision 6. No Vortex release ≥ 0.36 built on arrow 53, which ruled out pinning an
old Vortex.

Verified 2026-09-18 from crates.io: vortex 0.86.1 (2026-09-11, MSRV 1.95); datafusion 55.1.0
(2026-09-11, rustc 1.94); arrow-flight 59.3.0 (2026-09-01).

## Issue A — the rev (oracle-driven, lands FIRST)

Scope: root `Cargo.toml` `arrow = "53"` → `"59"`, `parquet` → 59, `arrow-flight` → 59 in
`cqlite-flight` and `tools/flight-loadgen`, DataFusion `=44.0.0` → `55.1.0` with the four
`cqlite-flight/src/df_spike/*` files ported to the 55 `TableProvider`/`ExecutionPlan` traits,
`rust-version` → `"1.95"` (workspace-inherited by every member) plus `.clippy.toml` `msrv`, and the
`dep-duplicates` baseline regenerated ONCE by `bash scripts/ci/check-dep-duplicates.sh --regenerate`.

Blast radius measured: ~110 Rust files referencing arrow/parquet/datafusion — 12 in
`cqlite-core/src/export`, 3 in `cqlite-cli/src`, 14 in `cqlite-cli/tests`, 71 in `cqlite-flight`
(src+tests+bench+example), 3 in `tools/flight-loadgen`, 3 in bindings. The arrow API in use is
mainstream (RecordBatch, builders, DataType/Field/Schema, `compute::concat_batches`, Parquet
`ArrowWriter`/`ParquetRecordBatchReaderBuilder`, `util::display`).

Commit order inside the one PR: (1) core export, (2) CLI, (3) Flight, (4) DataFusion spike —
each lite-gated; the spike last because it is the only part where "compiles" ≠ "done".

Rust-floor sweep (same PR): `Cargo.toml`, `.clippy.toml`, `CONTRIBUTING.md` (×2), `README.md`
(×3), `CLAUDE.md` troubleshooting line, `docs/development/{RELEASING,DEVELOPMENT}.md`,
`docs/performance.md`, and the website pages `installation.md` (×2), `python.md`,
`troubleshooting.md`, `use-cases/python-data-science.md`, `use-cases/nodejs-services.md`.
`docs/development/ci-toolchain-policy.md` already says workflows honor `rust-toolchain.toml`
(1.97.1), so CI needs no change; `future-rust-canary.yml` keeps tracking latest stable.

Acceptance = the EXISTING oracles pass unchanged: Parquet goldens (`cqlite-cli/tests/parquet_*`,
`issue_1490_parquet_*`), JSON/CSV golden parity (`issue_1491_*`), the whole `cqlite-flight` suite
(`flight-tests` unit lane + the 42 integration targets the census names), the Trino connector's
`docker-compose integration` job, and the Python/Node export tests. **A golden that changes bytes
is a FINDING, not an expected update.** The Trino Java side is untouched — Flight's wire format
is Arrow IPC, versioned by the protocol, not by the Rust crate.

Routing: oracle-driven (existing tests are the criterion) — GitHub issue + PR, no OpenSpec change.

## Issue B — Vortex export (design-driven, OpenSpec change `vortex-export`, branches from A's merged head)

### Writer
`cqlite-core/src/export/vortex.rs`, feature `vortex = ["arrow", "dep:vortex"]` (off by default,
like `parquet`); `cqlite-cli` gains `ExportFormat::Vortex` (`cli.rs`) and a `vortex` feature
forwarding the core one. Every existing `ExportFormat` match stays exhaustive (no `_ =>`), so the
new variant forces an arm at each dispatcher (`export.rs`, `export_sstable.rs`, `read_sstable.rs`
which rejects it exactly as it rejects Parquet). `delta-export`'s own `DeltaOutFormat` stays
Parquet-only — out of scope.

Data flow: the writer consumes the SAME `RecordBatch` stream `arrow_convert` already produces for
Parquet and Flight — no second CQL→Arrow mapping, no bridge (this is what the rev buys). Each
batch: `vortex_arrow::from_arrow_batch` → Vortex array → `session.write_options().write(...)` with
the default sampling compressor. Chunked shape mirrors `create_streaming_parquet_writer` /
`write_chunk` / `finalize`, so the CLI streams both formats identically and memory stays bounded at
one batch. Vortex features: `vortex-file` on; `vortex-cloud`, `vortex-tensor` off.

Type map: NONE added. Verified against `vortex-arrow/src/convert.rs` (2026-09-18): every Arrow
type `arrow_schema.rs` emits converts — Map, Struct, List, Decimal128(38,9), Date32, Time64,
Timestamp (temporal extensions), Utf8, Binary, Boolean, Int*/Float*. UUID/TimeUUID ride on the
`ARROW:extension:name = arrow.uuid` metadata cqlite already writes (`arrow_schema.rs:38`), which
Vortex's schema path (`vortex-arrow/src/uuid.rs`) recognises — a BARE FixedSizeBinary(16) would
be refused, so that metadata is load-bearing and must be pinned by a test. Arrow `Duration` is the
one type Vortex refuses (`unimplemented!`) and cqlite never emits it (CQL `duration` → Utf8).

Errors: fail closed — a conversion or write error aborts the export naming batch index + column,
leaving no partial file. No compression knobs exposed in this slice.

### Oracles (the #3042 rule: writer+reader from one crate proves nothing about cqlite)
- **Primary — cross-format differential**: for each of the 33 fixture tables, one `SELECT *`
  exported to Parquet AND Vortex; both read back to Arrow with their own readers; compared
  batch-for-batch on schema and values, full column set both directions (#3890). Parquet is
  golden-proven against sstabledump JSONL, so equality anchors Vortex to Cassandra-written truth
  transitively. Gate: named `--test` targets in a `write-tests`-style list, `CQLITE_REQUIRE_FIXTURES=1`.
- **Independent reader, CI only**: Python bindings test reads the `.vortex` with the `vortex`
  Python package and deep-equals against the JSON export. Same codebase, different linkage — the
  test DECLARES that at run time rather than claiming a second oracle. Lives in the
  `cross-binding-parity` tier, `@pytest.mark.slow`, for the reasons that harness records (#1455).
- **No byte goldens**: the sampling compressor chooses encodings per chunk; identical decode, byte
  drift. Neither a `.vortex` golden nor a decoded-value golden is added.
- **UUID extension pin**: a test asserting the exported Vortex dtype for a uuid column is the
  Vortex UUID extension, not `FixedSizeList<u8,16>` storage without the extension.

### Gate wiring
`feature-iso-vortex` lane: `vortex` WITHOUT `parquet` and WITHOUT `delta-scan`, mirroring the
existing mutual-isolation pair (`agent-gate.sh` `run_feature_iso*`). Every new `--test` target is
declared in `Cargo.toml` with `required-features` (the vacuous-pass hole roborev caught on #4196
job 3785). `features-load-bearing` is satisfied by the writer's `cfg` sites.

### Docs (move with B)
`website/.../output-formats.md`, `cli-reference.md`; `docs/development/dev-cookbook.md`
(`--features vortex` recipe); README formats table; release notes name Vortex as a new export
target and arrow 59 + Rust 1.95 as dependency/floor changes.

## Not in scope (named so nobody infers coverage)
Reading `.vortex` files (query/Flight/Trino input); `delta-export` to Vortex; compression
options; a physical-row (cell-metadata) export shape — that is #4222's raw view.

## Related
Epic #4192 (SSTable tool program); #4222 (raw SSTable view); #941 (DataFusion Design-A, whose
spike this rev ports); #1700 (`dep-duplicates` ratchet); #3042 / #3890 (oracle rules).
