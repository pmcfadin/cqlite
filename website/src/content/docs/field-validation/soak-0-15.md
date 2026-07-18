---
title: v0.15.0 Milestone Soak
description: A condensed ~2.5h field soak of the cqlite-flight v0.15.0 stack on a live 3-node Cassandra 5.0 cluster — 7 of 8 milestone claims verified, including the fix for the snapshot grace-sweep regression.
sidebar:
  label: v0.15.0 soak (2026-07-17)
  order: 1
---

# v0.15.0 milestone soak

A condensed (~2.5h) alternative to the 48–72h soak, run against a live 3-node Cassandra 5.0
cluster to answer one question before the milestone shipped: **is 0.15 sound?**

**Verdict: yes — 7 / 8 claims VERIFIED.** Every claim a condensed run can test passed. The
milestone's key open flag — the background snapshot grace-sweep (#2452) — is confirmed
fixed in the field. Multi-day stability (claim 8) is out of scope for a short run by design;
all its short-horizon proxies are green.

<a href="/cqlite/field-reports/soak-0.15/report.html" class="not-content" target="_blank" rel="noopener">**Open the full report →**</a>

The full report is a self-contained dashboard with four linked views:

- **[Verdict](/cqlite/field-reports/soak-0.15/report.html)** — the 8-claim table with evidence.
- **[Stability](/cqlite/field-reports/soak-0.15/stability.html)** — RSS, fd/thread, OOM/restart traces over the run.
- **[Phases](/cqlite/field-reports/soak-0.15/phases.html)** — per-phase breakdown (cold start, load, burst, drain, failover).
- **[Comparison](/cqlite/field-reports/soak-0.15/comparison.html)** — 0.15 vs the R11b / R12 baselines and the Lane-B goals ladder.

## What was verified

| # | Claim | Verdict |
|---|-------|---------|
| 1 | ~15× warm throughput (snapshot lifecycle, lazy index, point-read streaming) | **VERIFIED** — 90 min @ 32-thr: 211,373 queries, ~39 qps, p50 798ms, 0 client errors |
| 2 | Admission control under overload | **VERIFIED** — 80-thr burst: graceful queuing, not collapse; clean recovery |
| 3 | Saturation observability (blocking/egress/fd/threads/RSS) | **VERIFIED** — all gauges legible, returned to 0 after load |
| 4 | Snapshot retirement — background grace-sweep | **VERIFIED** — 660→6 snapshots at t=5min with **zero queries** (the R12 regression is fixed) |
| 5 | No silent row loss on large cells (≥1MB) | **VERIFIED** — integrity sweeps identical at 1,927,467 rows |
| 6 | Query-semantics correctness under concurrency | **VERIFIED** — 6 concurrent point-reads → 1 byte-identical result |
| 7 | Cold start / restart behavior | **VERIFIED** — cold first-query 2.2s, 0 index parses at boot, clean failover |
| 8 | Multi-day stability | **COULD-NOT-OBSERVE** — out of scope for ~2.5h; proxies all green (RSS flat 301–357Mi, 0 OOMKills) |

## Three things to report (none a milestone blocker)

- **Fan-out skew** caps throughput — a single pod is the ceiling (this is the one Lane-B
  goal, B2, not yet met).
- **0.89% server-side `do_get` error rate** — client-invisible, and down from 1.2% in R12.
- **A harness metrics-pipeline bug** inflated the dashboard's `rate()` panels ~1000× —
  found, fixed in the lab, and verified during this run; future runs render correctly.

> Run metadata: flight `v0.15.0@sha256:30c2b10c` (multi-arch INDEX) · connector 0.15.0 ·
> Trino 481 · Cassandra 5.0 RF=3 · 3× i4i.xlarge · ~1.93M partitions/node · 2026-07-17.
