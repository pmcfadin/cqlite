# Round-validation standard metrics (field round-reporting standard)

Canonical, per-round reporting template for the live 3-node field validation cycle run
against real Cassandra + Flight + Trino (the round tracker channel, e.g. #2367). Adopted
from the AWS field team's round-9 proposal (#2367, section "Proposed standard metrics",
2026-07-13) — 14 metrics grouped by what each protects, with **A/B baked into the
pass/fail round gate** and **C/D reported as tracked numbers**, exactly as the field
recommended. Issue: #2399.

**This is not `scripts/agent-gate.sh`.** The round gate below is a *live-cluster
field-validation* verdict (does this build behave correctly against a real 3-node
Cassandra + Flight + Trino deployment); `scripts/agent-gate.sh` is the pre-PR gate of
record for a code change (see [gate contract](https://pmcfadin.github.io/cqlite/agents-developing/gate-contract/)).
They are independent — a round can FAIL for reasons the agent gate cannot see (cross-node
fan-out, live cancellation, cluster hygiene) and vice versa.

Use `.github/ISSUE_TEMPLATE/round-tracker.yml` to open a new round tracker pre-populated
with this checklist. This document remains the source of truth for rationale and the
baseline; the issue template only references it.

## How to use this template

1. Open a new round tracker from the `round-tracker.yml` issue template (or copy this
   table into a fresh tracker issue).
2. Run the round against the pinned build (image digest, connector version).
3. Fill in every **GATE** row with a pass/fail; fill in every **TRACKED** row with the
   measured number. A round is a PASS only if every GATE row passes; TRACKED rows never
   block the verdict but must be present (a comparability gap is itself worth flagging).
4. Compare TRACKED numbers against the round-9 baseline below (and whichever round most
   recently superseded it) to catch regressions that don't outright fail a GATE item.

## A. Correctness — GATE

| # | Metric | What it protects | Local mirror | Round-9 baseline |
|---|---|---|---|---|
| A1 | Per-scenario **completes? + rows correct?** (LIMIT, count, `SELECT *`, point-read, tiny, mixed/TIME) in **both** snapshot and live mode | A silently-wrong or non-completing scenario shipping | field-only (live-cluster scenario matrix; see `easy-db-lab-kits/test-plans/`) | LIMIT ✅ 4m17s cold / 7.5s warm; cancel ✅; #2207 point-read ✅ 2.1s warm; `SELECT *` ✅ 1000→1000 rows 6.5s; #2227 multi-node ✅; #2193/#2229 no regression; count(*) ⚠️ completes but >10min (not a hang, dominated by #2385 cold parse) |
| A2 | **Row-count parity**: Trino `count(*)` vs Cassandra `nodetool tablestats` partition estimate | Silent short/0-row result (#2228 class) | field-only | not separately reported round-9 (folded into A1's `SELECT *`/count checks); `baseline: to establish` |
| A3 | **Access path** per query shape: `cqlite_read_partition_lookup_total{route,result}` + `cqlite_query_rows_scanned_total` — PK-equality must be a bounded lookup, never a full scan (#2207) | A point-read silently degrading into a full scan | `cqlite-flight/tests/point_read_route.rs`, `point_read_metrics_test.rs`, `metrics_capture_test.rs` | point-read warm: `rows_scanned≈60`, not 1.42M partitions — bounded lookup confirmed |

## B. Hang / liveness — GATE (the R7/R8 class)

| # | Metric | What it protects | Local mirror | Round-9 baseline |
|---|---|---|---|---|
| B4 | **Query wall time** cold vs warm, per shape | A hang shows as never-completes; latency creep shows as a growing number | field-only (reported latency table) | point-read warm 2.1s; `LIMIT 5` warm 6.3–7.4s; `LIMIT 100` warm 7.7s (fixed per-query cost, not per-row); `LIMIT 5` cold 257s (#2385) |
| B5 | **`cqlite_sstable_index_parses_total` delta per query** — MUST be ≤ #generations and flat on warm | The direct #2383 re-parse-storm regression guard | `cqlite-flight/tests/issue_2370_single_flight_test.rs`, `issue_2383_resolve_spin_test.rs`, `cqlite-core/tests/issue_2385_index_single_parse.rs` | `index_parses_total` = 22 total across the ENTIRE round (many queries), flat on warm (R8 storm: 8+ full re-parses in 90s from ONE query) |
| B6 | **`cqlite_rpc_phase_active_ratio{phase}`** during a scan — which phase is standing, and that it clears | The #2361/#2383 precision instrument for a stalled phase | `cqlite-flight/src/warm/spin_tests_2383.rs` (synchronous gauge unit tests) | resolve cleared within seconds on warm queries (R8: pinned at 8 the entire 12m29s hang) |
| B7 | **Cancellation reclaim time**: seconds from kill → pod CPU & memory back to baseline, WITHOUT a restart (report the number, not just pass/fail) | Cancellation not reaching a spinning loop (#2383) | `cqlite-flight/tests/issue_2383_resolve_spin_test.rs` (cancel-mid-parse pins) | ~15–60s kill → 1000m→1m CPU, memory drops, no DaemonSet restart (R8: stayed pinned 3+ min, only a restart reclaimed) |
| B8 | **Flight pod CPU + memory trajectory** during a scan — flat vs unbounded growth | A slow leak that only shows up under sustained load (R7 leaked to 5.5GB) | field-only (Grafana dashboard capture) | not separately reported round-9 beyond B7's post-cancel figures; `baseline: to establish` |

## C. Throughput / scale — TRACKED

| # | Metric | What it protects | Local mirror | Round-9 baseline |
|---|---|---|---|---|
| C9 | **qps, rows/s, p50/p99 latency, error rate** under a fixed loadtest (same threads × duration × query set each round) | Throughput regressions that no single-query latency check would catch | loadtest gating delegated to **#2377** (`easy-db-lab-kits/trino-loadtest/driver.py` `--gate` mode — nonzero exit on SLA breach); this change does not implement #2377 | ~0.9 qps aggregate (163 queries), ~30 rows/s, p50 9.4s / p99 17.7s, 0 errors (8 threads × 180s, warm point-read + `LIMIT 5`/`100` + tiny) |
| C10 | **Cross-node work distribution**: CPU across all N flight pods under load — all nodes participating, or single-node-bound? | Scale-out regressions hiding behind a healthy single-node number | field-only (per-pod CPU capture) | 🔴 single-node-bound — only 1 of 3 pods did any work (348m CPU vs 1m/1m idle); root-caused as #2397 (deterministic first-replica selection collapses under RF=N; connector-side fix pending) |
| C11 | **Cold-parse cost**: first-query-per-table wall time + `index_parses` (the #2385 baseline) | The one-time cold-start tax growing unnoticed | `cqlite-core/tests/issue_2385_index_single_parse.rs` | `LIMIT 5` cold = 257s at ~1.42M partitions (round-10/v0.14.1 already reduced this via #2385/#2395 — treat round-9's number as pre-fix baseline, not current) |

## D. Hygiene — TRACKED (binary)

| # | Metric | What it protects | Local mirror | Round-9 baseline |
|---|---|---|---|---|
| D12 | **Snapshot cleanup**: `nodetool listsnapshots \| grep cqlite-` == 0 on ALL nodes after queries | A leaked per-query Cassandra snapshot accumulating disk / confusing operators | **ADDED by this change**: `easy-db-lab-kits/trino-loadtest/driver.py` `--snapshot-check-cmd` (`find_leaked_snapshots` / `run_snapshot_leak_check`) — asserts zero `cqlite-` snapshots remain post-run; exits nonzero on a leak; reports `SKIPPED` (never a silent pass) when not configured | `baseline: to establish` (round-9 report did not include this figure) |
| D13 | **`cqlite_errors_total`** by category (should stay 0; note lazy registration — a 0 that was never incremented may not yet exist as a series) | Silent error accumulation | field-only (metrics scrape) | not separately reported round-9; `baseline: to establish` |
| D14 | **Digest pin verified**: every flight pod's `imageID` == the round's INDEX digest | A stale/mixed-version pod serving traffic during a round without anyone noticing | field-only (by inspection, `kubectl get pods -o jsonpath=...`) | `ghcr.io/pmcfadin/cqlite-flight:round9@sha256:4dfad8589d64912696b48600b7c64f1c09c26e420cfb8373fb95481453402e1c` (round-9 pin) |

## Field-only vs locally mirrored

Most GATE/TRACKED rows above are necessarily **field-only** — they require a live
3-node Cassandra + Flight + Trino deployment and are reported by a human operator each
round. Five rows already have (or, for D12, now have) an **in-repo local mirror** that
catches the same regression class between field rounds without a live cluster:

- **A3** — `cqlite-flight/tests/point_read_route.rs`, `point_read_metrics_test.rs`, `metrics_capture_test.rs`
- **B5** — `cqlite-flight/tests/issue_2370_single_flight_test.rs`, `issue_2383_resolve_spin_test.rs`, `cqlite-core/tests/issue_2385_index_single_parse.rs`
- **B6** — `cqlite-flight/src/warm/spin_tests_2383.rs`
- **B7** — `cqlite-flight/tests/issue_2383_resolve_spin_test.rs`
- **C11** — `cqlite-core/tests/issue_2385_index_single_parse.rs`
- **D12** — `easy-db-lab-kits/trino-loadtest/driver.py` (`--snapshot-check-cmd`; new, #2399)
- **C9** (partial) — loadtest gating is delegated to #2377, not yet implemented

The remaining rows (A1, A2, B4, B8, C10, D13, D14) stay field-only: they either need a
live multi-node cluster under real load (C10), depend on wall-clock/scenario coverage
that is the round's whole point to exercise (A1), or are cheap-enough manual inspection
that automating them is not worth the surface (D14).

## Baseline caveat

The round-9 numbers above are a **comparison anchor, not a pass threshold** — each
subsequent round supersedes them (e.g. the round-10/v0.14.1 build already improves C11's
cold-parse figure via #2385/#2395). Record the new round's numbers in its own tracker;
only note here if the canonical baseline itself should be bumped to a newer round.

## See also

- Round tracker channel: #2367 (the ongoing round-by-round report thread)
- Local mirror anchors: #2370 (single-flight/concurrency), #2377 (loadtest `--gate`
  mode), #2383 (resolve-spin + cancellation pins), #2385 (cold-parse)
- [Validation playbook](https://pmcfadin.github.io/cqlite/agents-developing/validation-playbook/)
  — the *local/CI* correctness oracles (sstabledump physical-dump parity, query-semantics
  parity); this document is the *live-cluster field-round* counterpart.
- `docs/development/pm-operating-loop.md` — the delivery operating model this reporting
  standard plugs into.
