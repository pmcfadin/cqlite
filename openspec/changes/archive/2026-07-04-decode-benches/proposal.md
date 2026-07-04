## Why

Nothing pins decode-level cost. The existing `read/type_heavy` perf-gate entry exercises the real v5
read path (good) but there is **no per-CQL-type decode bench, no rows/sec floor for wide all-primitive
rows, and no allocations-per-row / allocations-per-cell budget**. The July 2026 parser audit
(`docs/reports/parser-performance-audit-2026-07-01.md`, finding H2, audit block 2) calls this out:
Epics J and K claim large decode/allocation wins, and without these benches those claims are
unverifiable and their regressions invisible.

This is Epic H (#1601) "measurement + safety net", Wave 1 (measurement train). Design-driven
measurement-harness work — Seam-1 pre-approved for the batch. It coordinates with (and reuses) the
already-landed Epic A machinery: the criterion perf-gate (`perf-gate.json` +
`scripts/ci/check_perf_regression.py` + `.github/workflows/perf-regression.yml`), the unified append-only
history ledger (`cqlite-core/benches/bench_ledger/mod.rs`, A5 #1566), and the dhat allocation lane
(`cqlite-core/tests/memory_budget.rs` under the `dhat-heap` feature, run by the agent-gate
`memory-budget` component, A4 #1565). Parser decode-bench territory — disjoint from the concurrently
running read-cache work (B1 #1567).

Facts that constrain the design:
- The live block-path decode entry is `SSTableReader::parse_value_with_schema_type(&self, value_data,
  data_type)` (`cqlite-core/src/storage/sstable/reader/parsing/value_parsing.rs`). It is
  `pub(in crate::storage::sstable::reader)` — **not reachable from a bench crate**, so a bench must go
  through a thin, opt-in, `#[doc(hidden)]` bench-only public shim rather than a re-implemented copy
  (which would measure a different code path and violate the audit's intent).
- The scalar arms delegate to the authoritative standalone decoders; the collection/UDT/tuple/frozen
  arms recurse via `&self` and read `self.header.cassandra_version` — so the shim needs a real opened
  reader as its `&self` context. A single real CI-present fixture reader (`SIMPLE`, already used by A4)
  supplies that context; the per-type byte buffers themselves are fixed representative literals built in
  the bench.
- `dhat::HeapStats` exposes `total_blocks` (allocation count). allocs/row = `total_blocks / rows`;
  allocs/cell = `total_blocks / (rows * cols)` over a real full-scan of a wide real fixture — the honest
  measure of today's O(rows×cols) transient-string dispatch (finding J1).

## What Changes

- **Add an opt-in `bench-internals` feature** to `cqlite-core` (empty; enables a `#[doc(hidden)]
  #[cfg(feature = "bench-internals")] pub fn decode_value_for_bench(&self, value_data, data_type)` on
  `SSTableReader` that forwards verbatim to `parse_value_with_schema_type`). No default build, no real
  public API, no production-path change.
- **Add `cqlite-core/benches/decode_bench.rs`** (harness = false) with three criterion groups:
  - **`decode/type_<name>`** — for each CQL type (all scalars + `list`/`set`/`map`/`tuple`/UDT/`frozen`),
    decode a fixed representative byte buffer in a loop through the live entry (`decode_value_for_bench`
    → `parse_value_with_schema_type`); ns/op per type.
  - **`decode/wide_row_primitives`** — throughput (rows/sec) decoding a fixed in-memory block of a wide
    all-primitive row (~20 primitive typed columns) repeated to a row volume.
  - **`decode/text_heavy`** — throughput (rows/sec) on a block dominated by `text`/`blob` values (the
    K5/K6 measurement).
- **Reuse the unified history ledger**: the bench appends its metrics via `crate::bench_ledger`
  (best-effort; a ledger write never fails the bench).
- **Wire the criterion decode benches into the perf gate**: add `decode/wide_row_primitives` and
  `decode/text_heavy` to `perf-gate.json` (STRICT, ≥10% threshold); add `--bench decode` to the
  `perf-regression.yml` bench invocations (guarded by the existing "bench may not exist on base"
  pattern so the first landing stays green). Per-type entries are recorded/advisory (many small ns/op
  benches; the two throughput benches are the STRICT regression nets).
- **Add allocs/row + allocs/cell dhat budgets** to the A4 lane (`memory_budget.rs`): drive the real
  public `Database::execute` full scan over a wide real fixture, compute allocs/row and allocs/cell from
  `dhat::HeapStats::total_blocks`, and assert ≤ **current-main measured** ceilings (ratchet pattern —
  J1/K3 lower them later). Run by the agent-gate `memory-budget` component; fails closed on 0 rows.
- **Record the measured baseline numbers** (per-type ns/op, wide-row & text-heavy rows/sec, allocs/row,
  allocs/cell) on issue #1615 at landing.
- **Demonstrated red-run** (in the PR): show the allocs/row budget REDs when the ceiling is set below the
  measured value — the honest failing-on-regression evidence.

## Non-goals

- **No production decode-path change.** Additive bench/gate/test + one opt-in `#[doc(hidden)]` shim only.
- **No aspirational budgets.** Ceilings are pinned at current-main measured values so the gate is green at
  landing; J/K children tighten them as their fixes land.
- **No benching of dead code** (`optimized_complex_types`, `zero_copy_parser` — being deleted in J3).
- **No redefinition of the perf-gate mechanism** or the dhat lane beyond adding tracked entries/tests.
