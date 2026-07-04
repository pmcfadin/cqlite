## Context

Finding H2 wants decode cost pinned three ways: per-type ns/op, wide-row rows/sec, and
allocations-per-row / per-cell. The two hard constraints are (1) benches must exercise the **real** live
decode entry (`SSTableReader::parse_value_with_schema_type`), not a copy, and (2) the change must be
green at landing (ratchet) while faithfully showing today's bad allocation number.

## Key decisions

### D1 — Reach the crate-private decode entry via an opt-in `#[doc(hidden)]` shim, not a copy
`parse_value_with_schema_type` is `pub(in crate::storage::sstable::reader)`; a bench is an external
crate. Options: (a) re-implement the dispatch in the bench — rejected, it measures a different code path
and rots; (b) make the entry `pub` — rejected, it leaks parser internals into the stable API; (c) add an
empty `bench-internals` feature guarding a `#[doc(hidden)] pub fn decode_value_for_bench` that forwards
verbatim. **Chose (c)**: the real dispatch is measured, the surface is invisible (`#[doc(hidden)]`,
non-default feature), and default builds are byte-identical.

### D2 — Real opened reader supplies `&self`; per-type buffers are fixed literals
The scalar arms are `self`-independent, but the collection/UDT/tuple/frozen arms recurse via `&self` and
read `self.header.cassandra_version`. So the bench opens one real, CI-present V5 fixture reader (`SIMPLE`,
`test_basic.simple_table`, already vendored + used by A4) once, outside the measured region, and reuses it
as the decode context. The per-type byte buffers are fixed representative literals constructed in the
bench (e.g. `42i32.to_be_bytes()`, a short UTF-8 text, a 16-byte UUID, an assembled `list<int>` /
`map<text,text>` / tuple / frozen buffer). This keeps the per-type bench deterministic and independent of
any single fixture's column set, while still routing every decode through the live entry.

### D3 — Two throughput benches are the STRICT criterion gate; per-type entries are advisory
`decode/wide_row_primitives` (assemble a fixed ~20-primitive-column row buffer set, decode all columns in
a loop, `Throughput::Elements(rows)`) and `decode/text_heavy` (a text/blob-dominated block) are the two
STRICT `perf-gate.json` entries (≥10% median-regression). The many per-type `decode/type_<name>` benches
are numerous, tiny, and individually noisy — they are recorded (ledger) and left out of the STRICT set to
avoid flaky gating; their value is longitudinal tracking, not per-PR gating. This mirrors the existing
advisory/strict split.

### D4 — allocs/row + allocs/cell live in the A4 dhat lane, over REAL data, as absolute ratchets
The criterion gate is a *relative* PR-vs-base delta; an absolute allocation budget belongs in the dhat
lane (`memory_budget.rs`, `#[cfg(all(feature = "dhat-heap", feature = "cli-helpers"))]`,
`--test-threads=1`, `#[serial_test::serial]`). The budget drives the real public path
(`Database::execute("SELECT * …")`) over a wide real fixture — consistent with CQLite's "integration
tests use real SSTable data only" and A4's existing pattern — starts the profiler AFTER `open_read_db`
(so fixture-copy/ingest allocations are excluded), and computes:
- allocs/row  = `total_blocks / row_count`
- allocs/cell = `total_blocks / (row_count * column_count)`
It asserts each ≤ a pinned ceiling = **current-main measured value + variance slack**, and fails closed
(panics) if the fixture is present but yields 0 rows. The wide real fixture is `many_columns_table`
(`test_wide_rows`, 100 columns spanning every CQL type) when present, else the CI-guaranteed `SIMPLE`
fixture — selected via the existing `fixtures::fixture_present` guard so an absent optional fixture SKIPs
rather than fails, but a present-but-empty one panics.

### D5 — perf-regression.yml: guard the new bench like `compaction`
`--bench decode` is added to the PR bench run and, guarded by `[[ -f cqlite-core/benches/decode_bench.rs ]]`,
to the base (main) run. `check_perf_regression.py` already SKIPs any gated id missing from the base
baseline, so the first landing (no base data) stays green; subsequent PRs gate normally.

## Red-run (honest failing evidence)

The allocs/row budget is the honest part: today's dispatch allocates O(rows×cols) transient strings
(finding J1). The PR records the measured allocs/row and demonstrates that setting the ceiling below the
measured value makes the `memory-budget` lane RED (non-zero exit) — proving the budget catches a
regression — then restores the ceiling to the ratchet value so the gate is green at landing.

## Risks / mitigations

- **Per-type bench needs a fixture reader** → use `SIMPLE` (CI-guaranteed, already used by A4); skip
  gracefully only if entirely absent, panic if present-but-broken.
- **dhat lane is process-global** → reuse A4's `#[serial_test::serial]` + `--test-threads=1` discipline.
- **Ledger write failure** → best-effort; log to stderr, never fail the bench (existing `bench_ledger`
  contract).
