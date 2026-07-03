# Change: cold-open-ledger (Epic A / A5)

## Why

The July 2026 read-path audit (`docs/reports/read-path-performance-audit-2026-07-01.md`
§Epic A) is measurement-first: land the gauges before the optimizations so every
later claim (epics B–G) is pinned. A1–A4 landed the point-read gate, the tail
harness, the concurrency scaling floor, and the memory/layout lane. Three gaps
remain — this change (A5) is the capstone:

1. **No cold-open gauge.** Nothing benches `SSTableReader::open`/`Database` open
   cost (component loading: Statistics/Summary/CompressionInfo/BTI root) or the
   per-reader memory footprint. Epic G3 (bounded `Index.db` mode) needs the RSS
   baseline; there is none.
2. **The perf ledger is fragmented.** `scripts/profile_report.py` writes a
   run-summary `target/profiling/history.jsonl` from criterion medians, while the
   A2 tail harness writes a *separate* bespoke `benches/tail-latency-history.jsonl`.
   Two schemas, two files, neither read back for a longitudinal view. The audit
   calls for one persisted `history.jsonl`; A2's ledger is documented to
   "consolidate into Epic A5's unified `history.jsonl` when A5 lands."
3. **The work counters epics B–G need to write their TDD tests do not exist.**
   B1/C1/C3/C4/E3/E4/C2 each assert on trie-walk counts, decompress-call counts,
   seek counts, `open(2)` counts, and fd high-water marks — none of which is
   instrumented today. Without these gauges those epics cannot prove their wins
   the no-heuristics way (observe the *work*, not just the result).

## What Changes

- **Cold-open benchmarks** (`benches/open.rs`, additive; `harness = false`):
  - `open/cold` — time a fresh `Database` open (component loading from cold) on
    the BIG multi-chunk fixture and the BTI (`test_da`) fixture; skips when the
    fixture is absent, panics on present-but-broken (parity-is-truth).
  - `mem/open_n_readers` — open N readers and record heap/RSS after, so G3's
    bounded-Index.db mode has a before/after RSS gauge.
- **Unified append-only history ledger.** One schema — one JSON object per line,
  `{ts, commit, bench, metric, value, unit}` — written to a single
  `target/profiling/history.jsonl`. A shared Rust bench-support module
  (`benches/bench_ledger/mod.rs`) is the single append path for every A-series
  harness bench; `profile_report.py` writes criterion medians + peak heap in the
  same schema; `./scripts/profile.sh report` reads the ledger back and renders a
  longitudinal per-metric table (latest value + delta vs the previous commit).
  A2's `tail_latency` bench migrates onto it (its bespoke ledger is retired). The
  ledger is machine-specific generated run data: gitignored, uploadable as a CI
  artifact; documented in `docs/profiling.md` + `benches/README.md`.
- **Test-only read-work counters** in a new `read_work_counters` module, modeled
  on the existing `work_counters`/`SCAN_FOR_KEY_CALLS` pattern but **cfg-gated**
  (`#[cfg(any(test, feature = "work-counters"))]`) so release hot paths pay
  nothing: `TRIE_WALKS`, `DECOMPRESS_CALLS`, `SEEK_CALLS`, `FILE_OPENS`, plus an
  fd high-water-mark helper (platform-gated). Each has a reset + read API and a
  module-doc note naming the epic-child that consumes it. Increment calls are
  unconditional at their choke points but compile to a no-op in release.

## Impact

- Affected specs: **new** capability `read-perf-observability`.
- Affected code (all additive / measurement-only; **no behavioral production
  change**): `cqlite-core/benches/open.rs` (new), `benches/bench_ledger/mod.rs`
  (new), `benches/tail_latency/mod.rs` (migrate onto shared ledger),
  `cqlite-core/src/storage/sstable/read_work_counters.rs` (new) + a handful of
  cfg-gated increment call sites (decompress entry, BTI trie descent, block seek,
  BlockSource open), `cqlite-core/Cargo.toml` (new `work-counters` feature + bench
  entries), `scripts/profile_report.py`, `scripts/profile.sh`, `docs/profiling.md`,
  `cqlite-core/benches/README.md`, `.gitignore`.
- Risk: low. Counters are zero-overhead in release (cfg-gated no-ops); benches and
  ledger are dev-only; the only production-file edits are unconditional counter
  calls that vanish in release builds.
