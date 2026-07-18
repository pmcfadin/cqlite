---
title: Round 12 Field Validation
description: An observability-focused field round of the cqlite-flight stack on a live 3-node Cassandra 5.0 cluster — every R11b baseline held or improved, the new saturation gauges are visible under overload, and the large-cell fix is confirmed.
sidebar:
  label: Round 12 (2026-07-15)
  order: 2
---

# Round 12 field validation

An observability-focused round on a live 3-node Cassandra 5.0 cluster.

**Verdict: ALL GREEN.** Every R11b baseline held or improved, the round's headline — the
#2419 in-process saturation gauges — is now visible under overload where R11b showed only
zeros, and the #2436 large-cell fix produced a legitimate row-count increase. No restarts,
no OOM, no regressions.

<a href="/cqlite/field-reports/r12/report.html" class="not-content" target="_blank" rel="noopener">**Open the full report →**</a>

## Highlights

- **Regression parity vs R11b — held or better.** Warm `LIMIT`/point-read latencies sit at
  the ~JDBC floor, warm throughput and `count(*)` wall time at parity, zero cold index
  parses (#2412).
- **#2436 large-cell fix confirmed** — identical data returned **+8,489 rows** vs R11b:
  rows with a ≥~1MB single cell that were previously silently dropped now read correctly.
  A fix, not a discrepancy.
- **#2419 saturation gauges visible** — during an 80-thread scan overload the new
  in-process gauges produced a legible saturation trace (blocking tasks, egress channel
  depth, admission ratio, threads, fds, RSS), all rising under load and returning to
  baseline after drain. In R11b these read a flat, invisible 0.
- **Fan-out & memory bounded** — 8-thread load fans across all 3 flight pods; memory stays
  bounded (idle 3 Mi → ~603 MB heaviest pod at 80-thr overload), 0 OOMKills / 0 restarts.

> Run metadata: flight `round12@sha256:2433dde8` · connector 0.14.3 · Trino 481 ·
> Cassandra 5.0 RF=3 · 3× i4i.xlarge · ~1.93M partitions/node · 2 SSTable gens · 2026-07-15.
