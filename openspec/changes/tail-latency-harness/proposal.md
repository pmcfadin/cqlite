## Why

Every gated read-path perf number today is a Criterion **median** (issue #1562 landed the real
point-read median gate). But the July 2026 read-path audit
(`docs/reports/read-path-performance-audit-2026-07-01.md` §Epic A) found that its three biggest
read-path defects are all **tail** pathologies, not median ones:

- **C2 cursor convoy** — a background scan holds a cursor that serializes concurrent point reads;
- **F1 reader-map FIFO stall** — `table_readers.read().await` held across I/O stalls other readers;
- **F3 blocking I/O on async workers** — mmap page faults / `O_DIRECT` reads block a Tokio worker.

Each barely moves the median but inflates p99/p999 badly under a mixed load (a scan running while
point reads arrive). With no tail harness, none of those fixes can be shown red-then-green, and a
tail regression merges silently because the median gate cannot see it.

This is child **A2** of Epic A (#1513, "measurement first"; issue #1563). It is design-driven,
additive **measurement-harness** work — Seam-1 pre-approved for the Epic A batch. It builds directly
on A1 (#1562): the same real `WHERE pk = ?` partition-targeted point-read surface, the same
`benches/fixtures/mod.rs` loader, and the same `perf-gate.json`/checker conventions.

Guardrail (from the issue): **no production-code changes** — additive harness/gate/test only. Gate on
**ratios**, never wall-clock absolutes, so shared-runner noise cannot flap the gate.

## What Changes

- **A mixed-load tail-latency harness** (`cqlite-core/benches/tail_latency.rs`, a `harness = false`
  custom-main bench so it can emit percentiles instead of a single Criterion median). It:
  1. opens one shared `Database` over the BIG multi-chunk fixture (`test_basic.simple_table`, reused
     from A1) and starts **one continuous background full-table scan** (`SELECT *` in a loop);
  2. issues a fixed-length stream of real partition-targeted **point reads**
     (`SELECT id, name … WHERE id = <uuid-literal>`, the #949/#956 path A1 proved), recording per-op
     latency; and
  3. runs the identical point-read stream **with no background scan** as a **scan-free baseline**.
- **Machine-readable JSON output**: `{p50, p99, p999}` (nanoseconds) for the point-read stream under
  both the mixed load and the scan-free baseline, plus the derived gate ratios `p99_over_p50` and
  `p99_mixed_over_scan_free`.
- **Wiring-evidence**: the point-read stream drives the real public read path; setup asserts the query
  returns ≥1 row and reports a **targeted** `AccessPath` (`PartitionLookup`), never a full-scan
  fallback — otherwise the harness panics loudly (same honesty guards as A1).
- **A self-contained ratio gate, ADVISORY first**: `cqlite-core/benches/tail-latency-gate.json`
  (thresholds + an `advisory: true` flag) and `scripts/ci/check_tail_latency.py`, which reads the
  harness JSON, reports both ratios against their thresholds, and — while `advisory` is true —
  **always exits 0** (reports, never fails). A documented flip (`advisory: false`, or `--enforce`)
  turns it enforcing once C2/F1/F3 land.
- **Persisted ledger**: each harness run appends one JSON record (commit, timestamp, both stat blocks,
  ratios) to a history ledger (`cqlite-core/benches/tail-latency-history.jsonl`, gitignored run data).
  Documented to consolidate into A5's unified `history.jsonl` when A5 lands.
- **TDD tests** (`cqlite-core/tests/tail_latency_harness.rs`, run by the gate's `core-tests`
  `cli-helpers` component): (a) point-read p99 under mixed load ≤ `k` × the scan-free baseline p99,
  with `k` chosen from the first measured run on `main` and documented (this records the convoy as the
  "before" number); (b) determinism — two consecutive scan-free runs agree within a documented, wide
  tolerance; plus pure unit tests for the percentile math and the gate-ratio/advisory logic.

## Non-goals

- **No read-path production code changes.** Additive harness/gate/test only (issue guardrail). The
  actual C2/F1/F3 fixes are later Epic F children; this only makes them measurable.
- **No enforcing tail gate yet.** The gate ships advisory; flipping to enforcing is documented and
  deferred until the tail fixes land (so the recorded convoy ratio does not red the gate today).
- **No write-load axis.** The audit's tail findings are scan-vs-point-read read contention; a background
  write load is a documented future extension, out of scope here.
- **No change to the existing A1 median gate** (`check_perf_regression.py`, `perf-regression.yml`,
  `perf-gate.json`) — the tail gate is a separate, self-contained ratio checker.
- **Not** a cross-machine absolute-latency SLO; the harness gates intra-run ratios only.
