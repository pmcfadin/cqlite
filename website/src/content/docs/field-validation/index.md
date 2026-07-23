---
title: Field Validation
description: Real-cluster validation of the cqlite-flight + Trino stack — full reports over time, with the headline metrics tracked round over round.
sidebar:
  label: Overview
  order: 0
---

# Field Validation

CQLite's Flight server and Trino connector are exercised against **real Cassandra 5.0
clusters** — not fixtures — on every meaningful change. Each round runs the full stack
(cqlite-flight → Trino → live Cassandra) under load, captures Grafana panels server-side,
and produces a self-contained report.

This page tracks the **progression over time**. Each row links to the full, standalone
report for that round.

> **Common test bed:** 3× `i4i.xlarge` Cassandra 5.0 nodes (RF=3) + an `m5.xlarge` app
> node, ~1.93M partitions/node across 2 SSTable generations
> (`cassandra_easy_stress.keyvalue`), Trino 481.

## Reports

| Round | Date | Stack | Verdict | Report |
|-------|------|-------|---------|--------|
| **0.16.0 GA verification** | 2026-07-23 | flight 0.16.0 · connector 0.16.0 | **GA sound — all 3 tracked fixes hold** — #2806 split pruning fixed (−18% p50 / −4× CPU on keyed reads), #2807 UDT parse fixed, #2815 collection-column drop fixed (raw bytes; structured decode is a 0.17 follow-up) | [Full report →](/cqlite/field-validation/0-16-0-ga/) |
| **0.16.0-rc1 fix validation** | 2026-07-21 | flight 0.16.0-rc1 · connector 0.16.0-rc1 | **Soak ship-grade (0 errors, integrity holds); 2 of 3 testable fixes fail in the field** — #2679 split pruning inert (#2806), #2349 UDT blocked by a parser bug (#2807), #2681 abort taxonomy verified | [Full report →](/cqlite/field-validation/0-16-0-rc1/) |
| **v0.15.0 milestone soak** | 2026-07-17 | flight 0.15.0 · connector 0.15.0 | **7 / 8 VERIFIED** — the one open flag (background snapshot grace-sweep, #2452) confirmed fixed in the field | [Full report →](/cqlite/field-validation/soak-0-15/) |
| **Round 12** | 2026-07-15 | flight round12 · connector 0.14.3 | **ALL GREEN** — every R11b baseline held or improved; #2419 saturation gauges now visible under overload; #2436 large-cell fix confirmed | [Full report →](/cqlite/field-validation/round-12/) |

## Headline metrics, round over round

The stack keeps getting faster and cleaner. Latency numbers are **not** directly comparable
across rounds — R11b/R12 measured 8-thread load, the 0.15 snapshot ran at 32 threads — but
throughput, error rate, and resource behavior show the trend.

| Metric | Round 12 | v0.15.0 | 0.16.0-rc1 | 0.16.0 GA | Trend |
|--------|----------|---------|------------|-----------|-------|
| Warm throughput | ~33 qps @8-thr | ~39 qps @32-thr | ~35 qps @8-thr · ~32 qps @32-thr | ~33 qps @32-thr | parity |
| Keyed-read splits (point read) | — | — | 13 (pruning inert, #2806) | **1** (pruning fixed) | fixed |
| Keyed-read p50 (pruning on) | — | — | n/a (inert) | **152 ms** (−18% vs off) | new |
| `do_get` error rate | 1.2% | 0.89% | **0.026%** (genuine `internal`) | not re-measured | improving |
| Snapshot grace-sweep | query-triggered (738 held) | background (660→6, no query) | background (312→172, no query) | not re-run | HELD |
| OOMKills / restarts | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 | clean |
| Integrity (start == end) | — | 1,927,467 | **1,703,038** (new anchor, #2789 TTL) | not re-run | holds |

**What the trend shows:** the read path holds its ~JDBC-floor warm latency while the
server-side error rate falls every round, peak memory under overload dropped by roughly
half, and the snapshot-retirement regression that Round 12 flagged is fixed and verified
running in the background with zero query traffic. The one known gap — throughput ceiling
under high fan-out — is a documented single-pod skew, not a correctness issue.

<!-- Convention: newest round first. Give each new report page a lower sidebar.order
     than the previous one so it sorts above older rounds (this index stays at order 0).
     Add a row to both tables above and drop the standalone HTML under
     website/public/field-reports/<slug>/. -->
