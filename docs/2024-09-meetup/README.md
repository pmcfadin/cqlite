# CQLite 0.17 — Cassandra meetup talk (September 2026)

Working directory for the talk: the story, the claims we can make with a citation, the
cluster run that produces the charts (issue #4137), and the results as they land.

Layout:

| Path | What lands here |
|---|---|
| `README.md` | this file — thesis, claims, demo → chart map, run log, open questions |
| `results/` | raw series from the #4137 run, one CSV per demo (`d1-isolation.csv`, …) |
| `charts/` | one PNG per chart, titles carrying table row count + node shape + image digest |
| `REPORT.md` | the run report from #4137: pinned assets, corpus facts, one paragraph per demo |

Everything the #4137 agent produces lands here via PR, never as issue attachments.

## Thesis

Cassandra users pay for analytics on operational data with either an ETL pipeline or their
p99. CQLite reads the SSTables Cassandra already wrote, in place, from a snapshot, and serves
them over Arrow Flight into Trino. Three claims, each backed by one chart from the cluster run:

1. **Your app doesn't notice.** A full-table analytic through CQLite leaves Cassandra's client
   latency where it was. The same analytic through Cassandra's own CQL path does not.
2. **Same SQL, and it's faster.** The same Trino query over the same table is faster through the
   `cqlite` catalog than the stock `cassandra` catalog, on the same Trino.
3. **Fresh, bounded, cheap.** Visible within one flush interval; dashboard-class concurrency with
   bounded memory; cold start in seconds.

## Numbers we can already say (with the citation that goes on the slide)

All single-box, same-core, same-SSTable pairs unless stated. Warm unless stated.

| Claim | Number | Cite |
|---|---|---|
| Read path vs Cassandra `count(*)`, 1 physical core | **1.25×** (410,449 vs 328,623 rows/s) | #3100, `docs/reports/ws0-3100-report.md` |
| Ship every row vs Cassandra `SELECT *`, 1 physical core | **1.12×** (252,999 vs 225,771 rows/s) | #3100 |
| Served path across the 0.17 program | 0.29× (July, #3026) → 1.12× (#3100) ≈ **3.9×** gain, via the #3058 bypass | #3026 → #3100 |
| Memory per row while scanning | **~1.9 KB vs ~10 KB**, ~5× less | #3026, `docs/reports/ws0-cassandra-baseline-2026-07-27.md` |
| Through Trino, 3 pods, warm | **15×**: p50 2.9 s → 227 ms, p99 10 s → 366 ms, 34 qps @ 32 threads | #2367 round 11b |
| Overload | 0 OOM kills at 80 threads; idle 3–4 MiB | #2367 |
| Cold first `LIMIT` query | **257 s → 10.7 s** (lazy Summary-guided index) | #2412, round 10 |
| Box ceiling, bare scan, 6 physical cores | 2.73M rows/s at 93.5% marginal efficiency | #3299 (rig, c7i.4xlarge) |
| Served throughput, 6 physical cores | 1.17M rows/s | #3225 (rig) |

**Rig-only, never presented as cluster results:** the last two rows, and jemalloc's +29% /
+61% (#3551) — #4120 decides the shipping default; until it lands, say "measured, decision
pending" or leave it out.

**Not on a slide:** the single-core concurrency curve from #3100 (declines past N=2 per core);
the pre-#3058 merge path (82k rows/s); anything cold on EBS (cold I/O is ~60 µs/row page-in,
3× all producer CPU — cold numbers are storage numbers).

## Demo → chart map (issue #4137)

| Demo | Chart | Slide says | Caveat that must ride with it |
|---|---|---|---|
| D1 OLTP isolation | client p99 timeline, two shaded windows + GC-pause panel | "your app doesn't notice" | Flight pod shares the node's cores (no `resources.limits` on the DaemonSet); the claim is heap/GC/request path untouched |
| D2 same SQL, two catalogs | grouped bars per query, warm and cold as separate charts | "same SQL, N× faster" | medians of 3, elapsed from Trino query stats |
| D3 time-series SQL | latency per query, both catalogs | "analytics you can't write in CQL" | the SQL is the point |
| D4 concurrency ladder | qps and p99 vs threads (8→80) | "dashboards at scale, bounded memory" | R11b floor is the regression bar |
| D5 freshness | staleness over time, stock vs 10 s flush period | "fresh within a flush" | 0.17 freshness = flush cadence; memtable reads are 0.18 (#1807) |
| D6 cold start | one number | "seconds, not minutes" | vs 257 s history |
| D7 (opt) single-box refresh | updated 1.25× / 1.12× / memory-per-row | | existing rig, `scripts/perf/ws0-baseline.sh` |
| D8 (opt) Parquet export | rows/s, output MiB | "take a snapshot to your laptop" | |

## Decisions

- **Cassandra version: 5.0, not trunk.** Trunk writes BIG `pa` (default compat mode) and BTI
  `ea`; CQLite refuses `ea` and — until #4142 lands — would silently read `pa` as `oa`. Trunk is
  possible only under `storage_compatibility_mode: CASSANDRA_5`/`CASSANDRA_4` with BIG, and is
  unvalidated. If wanted, a separate "early look" after the 5.0 run.
- **Allocator: whatever the talk017 image defaults to.** No `LD_PRELOAD`.
- **Cluster shape:** 3× `i4i.2xlarge` db + 2× `m6i.2xlarge` app; never mix shapes in one chart.

## Run log

_(the #4137 agent appends one line per session: date, what ran, where the output landed)_

## Open questions

- Which of D7/D8 make the cut given cluster time (~1 day for D1–D6 at 50M rows)?
- Slide count and order — draft outline below once the charts exist.

## Slide outline (draft)

1. The problem: analytics on operational data = ETL or p99.
2. What CQLite is: reads Cassandra's SSTables in place, snapshot-isolated, Arrow Flight → Trino.
3. D1 chart: your app doesn't notice.
4. D2 chart: same SQL, two catalogs.
5. D3: SQL you cannot write in CQL — show the queries.
6. D4 chart: dashboards at scale.
7. D5 chart: freshness, honestly.
8. D6 + the 0.17 read-path program in one slide (1.25×, 1.12×, 3.9×, 5× memory, 15× Trino).
9. What's next: memtable freshness (0.18), allocator, columnar merge.
