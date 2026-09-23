# vortex-export — issue #4237 (depends on #4236, merged)

**Milestone:** 0.18. **Priority:** P1.
**Routing:** design-driven (OpenSpec + Seam 1) — a new public export surface (CLI flag + core
writer) with real latitude in data flow and gate wiring. The owner settled the product-level
decisions in a 2026-09-18 brainstorm, recorded in `docs/plans/2026-09-18-vortex-export-arrow59-design.md`
(PR #4235, merged) and issue #4237's own body. This proposal does not re-litigate those rulings; it
turns them into requirements.

## Why

`cqlite export --format parquet` already gives operators a columnar file for analytics tooling.
Vortex is a newer open columnar format (Vortex Data, format stable since 0.36) with a
sampling-adaptive compressor that competes with Parquet on read-time performance for the same
logical data. The owner's ruling: add it as a second export target using the exact same reconciled
`SELECT` row stream Parquet already consumes — no new query semantics, no new type system, no new
CQL→Arrow mapping. This is the smallest possible second-format slice: reuse the Arrow bridge #682
already built, add one writer.

Prerequisite #4236 (arrow 53→59, parquet 59, arrow-flight 59, DataFusion spike →55.1, Rust floor
1.85→1.95) is **merged** on `origin/main` (PR #4242, 2026-09-20; verified `arrow = { version = "59"
... }` and `rust-version = "1.95"` at HEAD `79db00db7`). Vortex 0.86.1 requires arrow 59 and MSRV
1.95, so this issue could not have started before that merge.

## What changes

**Library (`cqlite-core`).** `cqlite-core/src/export/vortex.rs` under a new `vortex = ["arrow",
"dep:vortex"]` feature, off by default (mirrors `parquet`). A shared `QueryRow`→Arrow `RecordBatch`
producer (an explicit adapter over the existing chunked query-export loop, not an assumption that
the current Parquet writer already exposes a batch stream) feeds both the Parquet writer and the
new Vortex writer from the SAME batches — no second CQL→Arrow mapping, no IPC bridge. Each batch
goes through Vortex's official Arrow-extension importer
(`session.arrow().from_arrow_record_batch(...)`, the path that sees `arrow.uuid` extension
metadata) then `session.write_options().write(...)` with the default sampling compressor. Vortex
crate features: `vortex-file` on, `vortex-cloud`/`vortex-tensor` off.

**CLI (`cqlite-cli`).** `ExportFormat::Vortex` added to the existing exhaustive enum in `cli.rs`,
forcing an explicit arm in every dispatcher that matches it today: `export.rs` (query export),
`export_sstable.rs` (`export-sstable --format vortex`), and `read_sstable.rs` (rejects Vortex with
the same message shape it already uses to reject Parquet). A `vortex` feature on `cqlite-cli`
forwards `cqlite-core/vortex`. `delta-export`'s `DeltaOutFormat` is untouched — Parquet-only,
explicitly out of scope.

**Gate.** A new `feature-iso-vortex` full-gate lane (vortex enabled, parquet and delta-scan off —
mutual isolation, matching the existing `feature-iso-parquet`/`feature-iso-delta-scan` pair) plus a
new lane that enables `parquet` AND `vortex` together to execute the primary cross-format oracle
(see design.md — no existing gate component enables both non-default features simultaneously
today, so this is new wiring, not a product decision).

## What this change must establish

1. **Vortex output equals Parquet output** for the same query, batch-for-batch, after normalizing
   physical chunk/row-group boundaries — schema (modulo extension-metadata spelling) and every
   value in the full column set, both directions (#3890). Parquet is already golden-proven against
   `sstabledump` JSONL, so this anchors Vortex to Cassandra-written truth transitively (#3042: a
   Vortex write+read round-trip alone proves nothing about CQLite correctness).
2. **UUID/TimeUUID survive the format boundary** via the `arrow.uuid` extension metadata CQLite
   already emits — pinned by a dedicated test, because a bare `FixedSizeBinary(16)` is refused by
   Vortex's importer.
3. **Row and field order are part of the contract**, not an implementation accident: token-ring
   partition order, schema-aware clustering-comparator order within a partition (composite
   clustering columns, per-column `DESC`, null/absent rules), and Arrow/Vortex field order from the
   query schema's column order — never `HashMap` iteration order.
4. **Fail closed, mid-stream**: a conversion or write error names the batch index and column,
   aborts, and leaves no `.vortex` file (or deletes an unreadable partial). No partial-file success.
5. **Wiring evidence from the binary**: `cqlite export --format vortex`, `cqlite export-sstable
   --format vortex`, and `read-sstable`'s rejection of Vortex all exercise the compiled CLI, not
   just library unit tests.

## Non-goals

- **Reading `.vortex` files** — query/Flight/Trino input, selective read. A later slice.
- **`delta-export` to Vortex** — `DeltaOutFormat` stays Parquet-only.
- **Compression options** — the pinned session default only; no CLI/library knob.
- **A physical-row / cell-metadata export shape** — #4222 (raw SSTable view) owns that; this change
  is reconciled-row (`SELECT`) semantics only, identical to Parquet.
- **Byte-goldens for `.vortex` output** — the sampling compressor is not byte-deterministic across
  runs; a review asking for a committed `.vortex` golden is answered by this line, not implemented.
- **A second independent-oracle claim** — the CI-only Python `vortex` package read-back is declared
  same-codebase-different-linkage at run time, never presented as validating against Cassandra.

## Impact statements (openspec rules)

- **No-heuristics (#28):** N/A to the writer itself — it consumes already-schema-resolved
  `QueryRow`/`RecordBatch` values from the existing reconciled query path; no byte-pattern type
  inference is introduced. The CQL→Arrow type mapping is unchanged (Vortex reuses the same
  `arrow_convert` output Parquet does).
- **Uncompressed-write claim boundary (#1406):** **not applicable.** #1406 governs the production
  *SSTable* write surface (flush/compaction via `SSTableWriter`) emitting only uncompressed BIG/BTI
  components. A `.vortex` file is not an SSTable component and does not go through
  `SSTableWriter`; #1406's claim boundary is unaffected and this change makes no compressed-SSTable
  claim of any kind.
- **Cassandra 5.0 only (na/nb BIG, da BTI):** unaffected — Vortex export reads through the existing
  reconciled query path, which is already bound by `BigVersionGates`/`BtiVersionGates`. No new
  SSTable-version-aware code is added.
- **Oracles — Cassandra-written bytes / sstabledump, never CQLite round-trip alone (#3042):** the
  primary oracle (item 1 above) is a cross-format differential anchored to the Parquet leg's
  existing sstabledump-golden provenance, not a Vortex-write-then-Vortex-read round-trip. The
  CI-only independent-reader test is declared as same-codebase evidence, not a second oracle.
- **Public surfaces:** `cqlite-core` gains `pub mod vortex` under `export` (feature-gated,
  unconditional `pub mod` per the crate-root pub-surface guard — #1712); `cqlite-cli` gains one
  `ExportFormat` variant and a forwarding `vortex` feature. Python/Node bindings untouched (no
  `export_vortex` binding method in this slice — CLI-only, per the design doc's export-only,
  CLI-first scope).
- **Gate:** new `--test` targets declared with `required-features` in `Cargo.toml` (#4196's job
  3785 vacuous-pass lesson, named in this issue's own acceptance criteria); `features-load-bearing`
  and `dep-duplicates` (no `ADVISORY-INCREASE`, vortex's `parquet ^59.2`/`tokio` deps are already
  in the tree post-#4236) both green.
