# Design: cold-open-ledger (A5)

## Context

Measurement infrastructure only. A1–A4 established the conventions this change
reuses (do not duplicate their benches): fixture-present skip / present-but-broken
panic; ratios/counters over wall-clock absolutes; `cli-helpers` gates read benches;
`perf-gate.json` is intra-run. A5 adds the three remaining gauges.

## Decision 1 — Counter gating: unconditional call, cfg-gated body

The existing `work_counters` sit on *cold per-lookup boundaries* and are therefore
always-on. A5's counters (`DECOMPRESS_CALLS`, `SEEK_CALLS`) sit on *per-chunk /
per-seek* paths that are much hotter, and the issue's DoD demands "zero overhead in
release builds (cfg-gated)". Pattern:

```rust
#[inline(always)]
pub fn record_decompress() {
    #[cfg(any(test, feature = "work-counters"))]
    DECOMPRESS_CALLS.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
}
```

- **Call sites are unconditional** (`read_work_counters::record_decompress();`) —
  no `#[cfg]` scattered through production code, so the read path reads identically
  in every build. In release (no `work-counters`, no `test`) the body is empty and
  the `#[inline(always)]` no-op is optimized away → **zero overhead**, satisfying
  the guardrail without touching behavior.
- **Getters + `reset()` are cfg-gated** (`#[cfg(any(test, feature = "work-counters"))]`)
  — only test/feature builds read the values. In-crate unit tests get them via the
  `test` cfg; integration tests in `tests/` and benches enable the `work-counters`
  feature (they compile the lib without its `test` cfg, same reason
  `SCAN_FOR_KEY_CALLS` exists — see its doc). A local-`Counters`-instance unit test
  (per issue #1071) gives deterministic absolute-value assertions immune to
  parallel tests mutating the process-global.

New Cargo feature `work-counters` (off by default, not in any default set):
enables the getters/`reset` and the counter bodies for benches/integration tests.

## Decision 2 — Counter choke points (minimal, unambiguous, documented)

Instrument the single well-defined choke point for each counter rather than every
scattered `.seek`/`open` (which would be fragile and risk behavioral edits):

| Counter | Choke point (increment site) | Consumer epic-child |
|---------|------------------------------|---------------------|
| `TRIE_WALKS` | BTI trie descent entry (`data_access/bti.rs` walk fn) — one per descent | C3 (single-walk), C4 (hoist rehash) |
| `DECOMPRESS_CALLS` | `Compressor::decompress` entry (`compression.rs`) — one per chunk decompress | B1 (chunk cache), E3 (copy-chain) |
| `SEEK_CALLS` | the block-read seek in the chunk read path (`reader/block_io.rs`) | E4 (drop redundant seeks) |
| `FILE_OPENS` | `BlockSource` file-open site(s) (`reader/source.rs`) — one per `open(2)` | C2 (pread / kill per-lookup open) |
| fd high-water helper | `read_work_counters::fd_high_water()` — reads `/dev/fd` (macOS) or `/proc/self/fd` (Linux), returns count | C2 (fd-exhaustion guard) |

The fd high-water mark is a **helper**, not an atomic: it samples the process's
open-fd count at call time (platform-gated; returns `None` on unsupported OS so a
test can `skip`). Tests capture it before/after to bound fd growth.

Each counter's module doc names its consumer (above), so a future epic finds its
gauge by grepping the counter name — the same discoverability the existing
`work_counters` doc provides.

## Decision 3 — One ledger schema, one file, one writer module

**Schema:** one JSON object per line —
`{"ts": <unix_secs>, "commit": "<sha|unknown>", "bench": "<id>", "metric": "<name>", "value": <number>, "unit": "<str>"}`.
One record **per metric** (issue step 3), so a run of the tail harness emits e.g.
`tail_latency`/`mixed_p99`, `tail_latency`/`p99_over_p50`, … as separate lines, and
a `read` criterion group emits `read/get_partition_big`/`median_ns`, etc.

**Path:** `target/profiling/history.jsonl` (the path `profile_report.py` +
`docs/profiling.md` already document), resolvable from a Rust bench via env
`CQLITE_BENCH_LEDGER` else `<CARGO_MANIFEST_DIR>/../target/profiling/history.jsonl`.
Gitignored (machine-specific run data); the old
`benches/tail-latency-history.jsonl` ignore line is removed with its producer.
CI may upload it as an artifact (documented) — we do not commit it (a committed,
per-machine, churning ledger would be noise and a merge-race magnet).

**Single writer module** `benches/bench_ledger/mod.rs` (Rust, shared via `#[path]`
by each harness bench, like `tail_latency/mod.rs` today):
`append_metrics(bench: &str, metrics: &[(&str, f64, &str)]) -> io::Result<()>`
resolves the path, stamps `ts` + `commit` (`GIT_COMMIT` env override else
`git rev-parse HEAD` else `"unknown"`, reusing A2's `current_commit`), and appends
one line per metric. Append is best-effort: a failure logs to stderr and never
aborts the bench (a ledger write must not fail a measurement run).

`profile_report.py` writes the **same** schema for its criterion medians + peak
heap (replacing its old bespoke `{ts,rev,benches{},peak_heap}` summary line), and
`./scripts/profile.sh report` reads the whole ledger back into a longitudinal
per-metric table (latest value + delta vs the previous distinct commit). A
round-trip test proves write→read.

### Alternatives considered
- *Keep two ledgers, teach `report` to read both.* Rejected: two schemas is exactly
  the fragmentation A5 is chartered to remove.
- *Commit the ledger.* Rejected: per-machine timing data churns every run and would
  collide on `main` (the delivery-telemetry merge-race lesson); artifact/gitignore
  is the honest choice for generated run data.
- *Atomic fd counter instead of a sampling helper.* Rejected: wrapping every
  `open`/`close` to keep a live fd count is invasive and racy; sampling
  `/dev/fd`|`/proc/self/fd` at test checkpoints is what C2's guard actually needs.

## Risks / Trade-offs

- **Bench compile cost / feature matrix.** The `work-counters` feature adds one
  build config; the agent gate already builds several. Mitigated by keeping the
  feature tiny (getters+reset only) and off by default.
- **fd helper portability.** `/dev/fd` (macOS) and `/proc/self/fd` (Linux) only;
  returns `None` elsewhere so the test skips rather than fails — no false red on an
  unsupported CI OS.
- **Ledger path under `target/`.** Wiped by `cargo clean`; acceptable — it is a
  run ledger, not a source artifact, and CI captures it as an artifact when needed.

## Migration

`tail_latency/mod.rs` drops its private `LedgerRecord`/`append_ledger`/
`default_ledger_path` and calls `bench_ledger::append_metrics("tail_latency", …)`;
the `tail_latency_harness` integration test is unaffected (it does not assert on the
ledger file). The `.gitignore` line for the old file is removed.
