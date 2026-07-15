---
title: Performance
description: Measured cqlite-flight field results (round 11b, 2026-07-15) and the owner-ratified performance goals ladder — analytics on live Cassandra SSTables with zero ETL, no OLTP impact, and seconds-fresh data. Measured numbers vs targets we are running toward.
sidebar:
  label: Performance
  order: 9
---

# Performance

CQLite's Arrow Flight server (`cqlite-flight`) and Trino connector (`cqlite-trino`)
turn a Cassandra cluster's on-disk SSTables into a distributed SQL surface. The
positioning is a trifecta:

- **Zero ETL.** The data queried *is* the live Cassandra data on disk. There is no
  pipeline to run, no second copy, and no export lag — tombstone / TTL / last-write-wins
  reconciliation is the read path's job, already solved.
- **No OLTP impact.** Reads go through Cassandra **Sidecar** hardlink snapshots off
  disk, off the JVM heap, and never touch Cassandra's own request path — so analytics
  load does not contend with operational serving.
- **Seconds freshness.** Snapshot cadence bounds staleness to a few seconds; there is
  no streaming lag to reconcile.

This page reports the **measured** field results and the **target** goals we are running
toward. The two are kept visually distinct: measured numbers carry a date and the exact
cluster setup; targets are goals and are never phrased as achieved.

## Measured — round 11b field run (2026-07-15)

These are field-run numbers from the AWS-team round-11b run on issue
[#2367](https://github.com/pmcfadin/cqlite/issues/2367).

**Setup:** 3 flight pods · Cassandra 5.0 RF=3 · ~1.94 M partitions/node · 2 SSTable
generations · snapshot mode · connector `0.14.3` · image `round11b` · Trino 481.

| Metric | Measured (R11b, 2026-07-15) |
|--------|------------------------------|
| Warm interactive (8-thread, through Trino) | **p50 227 ms · p99 366 ms · ~34 qps** · 0 errors |
| Server-side read cost | **~2 ms** cumulative over 9 calls (the ~2.3 s wall time is Trino/JDBC coordinator floor, not engine cost) |
| Warm `LIMIT 5` / `LIMIT 100` | 2.3–3.0 s ≈ coordinator floor |
| Point read (`WHERE pk=`) | 2.4–3.3 s ≈ JDBC floor; bounded index lookup |
| Cold first-touch full index parses | **zero** (`index_parses_total` and `index_interval_parses_total` absent — open is O(summary)) |
| `count(*)` full ring | 1,939,286 rows in 66.2 s across 3 pods |
| Flight pod memory | **3–4 Mi idle · < 400 Mi peak** (270–391 Mi under 8-thread load) |
| Overload stability | **0 restarts, 0 OOMKills** under an 80-thread scan-heavy overload |

The warm interactive throughput reflects per-`(keyspace, table)` snapshot reuse
(issues [#2306](https://github.com/pmcfadin/cqlite/issues/2306) /
[#2356](https://github.com/pmcfadin/cqlite/issues/2356)): a snapshot is shared across
queries within a freshness window instead of taken per query.

## The goals ladder — targets we are running toward

The tables below are the **owner-ratified performance goals** (epic
[#2403](https://github.com/pmcfadin/cqlite/issues/2403), 2026-07-15), anchored to the
R11b baselines above. The **Baseline** column is measured; the **Target** and **Stretch**
columns are goals — where we are running, **not** what the code achieves today.

### Lane A — server-direct Flight (engine-owned)

The server-direct path bypasses the Trino coordinator, so its floor is set by CQLite's
own read path rather than the gateway.

| ID | Goal | Baseline (R11b) | Target | Stretch |
|----|------|-----------------|--------|---------|
| A1 | Warm keyed read | server ~2 ms (wall unmeasured without Trino) | p50 ≤ 10 ms, p99 ≤ 100 ms | p99 ≤ 50 ms |
| A2 | Keyed throughput | — | ≥ 1,000 qps/pod warm | 5k/pod |
| A3 | Cold first-touch per table | O(summary) | ≤ 250 ms | ≤ 100 ms |
| A4 | Scan feed rate | ~10k rows/s/pod | Stage 1 ≥ 100k/pod → Stage 2 ≥ 600k/pod | Stage 3 millions/s (columnar) |
| A5 | Overload stability | 0 restarts @ 80-thread | regression floor | — |

### Lane B — through-Trino (floor-shared)

The through-Trino path shares Trino's coordinator floor (planning + split sourcing +
collection stage), so its interactive minima sit in the hundreds-of-ms band regardless
of connector.

| ID | Goal | Baseline (R11b) | Target |
|----|------|-----------------|--------|
| B1 | Warm interactive | p50 227 ms / p99 366 ms | ≤ 300 / ≤ 500 ms = regression floor |
| B2 | Concurrency | 34 qps @ 8-thread | ≥ 100 qps @ 32-thread, 3 pods |
| B3 | Full-scan 1.94 M rows | 66 s | Stage 1 ≤ 10 s → Stage 2 ≤ 3 s |
| B4 | Freshness / memory | ≤ 3 s · 391 Mi peak · 3–4 Mi idle | floors: ≤ 3 s · ≤ 512 Mi · ≤ 16 Mi |

### What the stages require (owner-acknowledged)

The ladder forces an explicit engineering decision when each stage saturates:

- **Stage 1** is the current 0.15 remit (this epic + the saturation program,
  [#2313](https://github.com/pmcfadin/cqlite/issues/2313)).
- **Stage 2** requires promoting the DataFusion projection/pushdown provider
  ([#941](https://github.com/pmcfadin/cqlite/issues/941)).
- **Stage 3** (columnar, millions of rows/s) requires the ArrowMemtable OLAP work
  ([#2037](https://github.com/pmcfadin/cqlite/issues/2037)).

## Honest scope — what is in-class today vs on the ladder

CQLite-flight is the **storage feed**, not the analytic engine: Trino owns SQL —
aggregation, joins, `GROUP BY`, window functions — while the Flight server turns a
token-range scan of live SSTables into Arrow batches, already reconciled for
tombstones / TTL / LWW. The usability of a workload reduces to whether the feed can
deliver the rows that workload needs, fast enough and fresh enough, without asking the
feed to do work it structurally cannot (columnar projection, non-key predicate
pushdown, metadata-only counts).

**In-class today — bounded / in-grain analytics.** Dashboards, drill-downs, keyed
lookups, and time-series rollups that read a bounded slice of a partition or a bounded
`LIMIT-k` land on the warm/floor path: server-side cost is ~2 ms, and wall time is
dominated by the Trino coordinator floor. These are usable now.

**On the ladder — heavy aggregations.** Full-table aggregations and large scans are fed
**row-oriented** today: the feed rate is row-count-bound (a `WHERE non_key_col = …`
scan feeds the same rows and Trino filters afterward), so multi-million-row aggregations
are the ceiling. Columnar projection and pushdown are Stage 2/3 roadmap items above. For
warehouse-style aggregation workloads today, the recommended tier is the **Parquet
export** path (feature `parquet`, epic
[#682](https://github.com/pmcfadin/cqlite/issues/682)) into a lakehouse — accepting an
export pipeline and its freshness lag in exchange for columnar scan performance.

This framing is developed further in a set of forthcoming architecture memos
(`perf-class-marks-2026-07`, `htap-positioning-2026-07-landscape`,
`htap-positioning-2026-07-usability`, `parquet-backend-comparison-2026-07`), which
place cqlite-flight in the HTAP / hybrid-analytics landscape and compare it against a
Parquet backend.

### Claim boundaries

- **Single-replica reads.** A Flight read serves one replica's SSTables; it does not
  perform quorum-style cross-replica reconciliation. Results reflect that replica's
  on-disk state. Consistency semantics for the connector are tracked in
  [#1045](https://github.com/pmcfadin/cqlite/issues/1045).
- **Reads flushed data only.** The server reads SSTables Cassandra has already flushed
  to disk; rows still in a memtable are invisible until flushed. See
  [Read surfaces & freshness](/cqlite/user-docs/read-surfaces-and-freshness/).
- **No compressed-SSTable write claims.** CQLite's production write surface emits
  **uncompressed** SSTables only; compressed writing is unwired
  ([#1406](https://github.com/pmcfadin/cqlite/issues/1406)). Nothing on this page depends
  on compressed writes.
- **Parity claims.** Correctness parity vs Apache Cassandra is gated by the
  [parity release checklist](https://github.com/pmcfadin/cqlite/blob/main/docs/development/parity-release-checklist.md)
  — this page makes no parity claim beyond what that process gates.

## Method note

The measured numbers on this page come from the AWS-team field rounds recorded on issue
[#2367](https://github.com/pmcfadin/cqlite/issues/2367); round 11b is the current
baseline. The measurement rig for the server-direct (Lane A) lanes is **flight-loadgen**
([#2418](https://github.com/pmcfadin/cqlite/issues/2418), forthcoming), which drives the
Flight server directly to measure engine-owned latency and throughput under the Trino
coordinator floor. Each round-N report re-stamps the Lane A / Lane B tables against fresh
field baselines.
