# Is Trino → cqlite-flight usable for analytics today? — a workload-class usability read (2026-07)

**Status:** Research memo. Companion to `parquet-backend-comparison-2026-07.md` (which weighs
cqlite-flight vs a Parquet/lakehouse *backend*). This memo asks the narrower product question:
**with Cassandra as the OLTP system of record, which analytic workload classes is the
Trino → cqlite-flight path usable for *today*, and which roadmap items move each verdict.**
**Audience:** owner + maintainers sizing the 0.15 "latency/throughput/ops" theme (#2403) against
real analytic demand.
**Baseline:** round-11b field run, issue #2367, 2026-07-15 (see that thread). Not duplicated here.

Marking: **[M]** MEASURED (round-11b) · **[R]** REASONED (structural, from the read path's shape).

## Framing: our path is the storage feed, not the analytic engine

Trino owns SQL — the aggregation, the joins, the GROUP BY, the window functions. cqlite-flight is
the **storage feed**: it turns a token-range scan of live SSTables into Arrow batches, already
reconciled for tombstones / TTL / LWW across generations. So "is it usable for workload X" reduces
to one question: **can the feed deliver the rows workload X needs, fast enough and fresh enough,
without asking the feed to do work it structurally cannot** (columnar projection, non-key predicate
pushdown, metadata-only counts). Everything below follows from that.

Two numbers set every verdict:
- **Bounded/warm path [M]:** warm `LIMIT-k` and point reads sit at the ~2.3 s Trino/JDBC floor;
  server-side cqlite cost is ~2 ms. Under 8-thread warm load: **~34 qps, p50 227 ms, p99 366 ms,
  0 errors** (snapshot-reuse, #2306/#2356). Freshness ≤ 3 s.
- **Full-scan feed rate [M]:** `count(*)` full-ring = **1,939,286 rows in 66.2 s ≈ 29.3k rows/s**
  across the 3-pod cluster (~9.8k rows/s/pod), fanning across all 3 pods post-#2397 rotation.

### What the feed rate means for table sizing (REASONED from the [M] rate)

At ~29.3k rows/s for this 3-pod cluster, a **full-table** scan-and-feed finishes in:

| Under | Rows the cluster can full-scan-feed in that budget |
|---|---|
| **1 min** | ~1.8 M rows |
| **5 min** | ~8.8 M rows |
| **15 min** | ~26 M rows |

Two caveats that matter for reading this table: (1) the rate is **row-count-bound, not
selectivity-bound** — a `WHERE non_key_col = …` scan feeds the *same* rows (Trino filters after),
so a "selective" report over a big table still pays the full-scan clock. (2) The threshold scales
~linearly with **cluster node count** (more token ranges, more pods feeding in parallel), so a
12-node ring roughly quadruples these row budgets — but the **per-ring feed rate is fixed** and no
amount of node-scaling adds columnar projection or a metadata count. This is the ceiling (§ below).

## The usability matrix (the deliverable)

Verdicts: **USABLE TODAY** / **MARGINAL** (works, but feed-rate- or freshness-constrained; size-gated)
/ **NOT YET** (structurally feed-bound today; needs a roadmap item). "Bounded" = the analytic slice
maps to a partition key or a token/clustering range, so the feed does an **indexed seek**, not a scan.

| Analytic class | Verdict today | Binding bottleneck | The roadmap item that flips it |
|---|---|---|---|
| **Operational dashboards** — repeated aggregations over a *recent, partition-scoped* slice | **USABLE** [M] | none at the bounded/warm path: 34 qps, p50 227 ms, freshness ≤3 s is the actual selling point vs a lakehouse | already there; #2313 hardens it under many concurrent dashboards |
| **…over a *cross-partition* recent window** (e.g. "last 5 min across all keys") | **MARGINAL** | not partition-bounded → full/large scan feed; no non-key (time) pushdown unless time is the clustering key | #941 (projection+predicate pushdown), #2366/#2230 (feed-rate) |
| **Ad-hoc drill-down / slice** — selective predicate | **USABLE** when the predicate is on the **partition key / token range** (indexed seek, ~2.3 s) [M]; **MARGINAL** when it's on a **non-key column** (whole table feeds, Trino filters) [R] | no non-key predicate pushdown; no column projection → full row feed regardless of selectivity | #941 DataFusion provider (pushes projection + filters + limit into the scan) |
| **Large full-table aggregation / reporting** | **MARGINAL to ~8.8 M rows** (≤5 min) / **NOT YET** beyond ~26 M rows [M/R] | full LWW merge, row-oriented, no metadata-only `count(*)`, no columnar projection | #2037 ArrowMemtable (columnar post-merge) or the delta-export Parquet tier (#696/#705) |
| **Joins across tables** | **MARGINAL** for small-dim × large-fact (broadcast the small side); **NOT YET** for large × large [R] | each side is an independent full row feed; no projection to shrink bytes shipped to the Trino shuffle | #941 (projection cuts join input) then #2037 (columnar) |
| **Time-series rollups** — *single/bounded series* (partition = series, clustering = time) | **USABLE** [R] — a time-range rollup within a partition is a clustering-range seek | none at the bounded path | already there |
| **…rollup across *all* series** over a time range | **MARGINAL/NOT YET** at scale [R] | cross-partition → full-scan feed; the time predicate isn't a partition selector | #2366/#2230 feed-rate, then #2037 columnar |

**Reading it:** the diagonal is clear — **anything the Cassandra data model already makes
bounded (a partition or token/clustering range) is usable today and genuinely good** (sub-second
server work, ≤3 s freshness, 34 qps). Everything that requires touching a **large fraction of the
ring** is gated by the row-oriented full-scan feed rate and the absence of columnar projection /
metadata counts. cqlite-flight is a **live operational-analytics gateway that is excellent inside
the grain of the data model and size-limited outside it.**

## Roadmap deltas — which cell each flips, and how mature

Two kinds of item: **incremental** (raise the feed rate / remove cliffs / harden concurrency — push
the size thresholds up, but stay row-oriented) and **structural** (cross the row→columnar line —
the only thing that flips the heavy-OLAP NOT-YETs).

**Incremental — push the MARGINAL boundary, don't cross the line:**
- **#2403 remaining lane levers** — #2397 replica rotation (VERIFIED round-10: 3/3 pods, ~15× warm
  throughput), #2385/#2412 lazy Summary-guided index (VERIFIED: cold cliff gone, zero full parses),
  #2398 warm-setup collapse (SHIPPED). Net effect [M]: they took the bounded/warm cells from
  MARGINAL to solidly **USABLE** and pushed the full-scan clock down (cold `count(*)` 79→66 s).
  *Maturity: mostly landed — this is why "today" is as good as it is.* Remaining: #2366 O(partitions)
  random-read cliff, #2230 whole-partition materialization — both raise the full-table row budget
  further, still row-oriented. *Near.*
- **#2313 saturation program** — removes the concurrency plateaus (per-query `Runtime::new()` thread
  amplification, no admission control, fd exhaustion, unbounded RSS). Flips **"many concurrent
  dashboards/analysts"** from MARGINAL to USABLE — it is throughput-under-load, not per-query feed
  rate. Admission control (#2420) already landed. *Near–mid.*

**Structural — the actual answer for the heavy-OLAP end:**
- **#941 DataFusion table provider** (Design-A colocated-over-Flight packet in-tree) — pushes
  **projection + filters + limit into the scan**. This is the first item that makes a *selective*
  or *narrow-column* query stop paying the full-row-feed price: it flips the **non-key drill-down**
  and shrinks **join** inputs. Still sources rows from the row-oriented read path, so it lowers the
  constant, not the asymptote. *Mid — design packet exists, Backlog-by-design, owner-gated.*
- **#2037 ArrowMemtable coordinator-native OLAP** — the structural answer for full-table
  aggregation / joins / all-series rollups. Produces **Arrow batches post-merge, cached per
  generation** (convert each byte once per generation, not once per query), giving columnar
  projection and late materialization *after* Cassandra semantics are resolved. This is what flips
  the **NOT-YET** cells. *Far — exploration/Backlog, gated on WS7 (#2043) benching the flip-risk
  constants; do not promote.*
- **delta-export Parquet tier (#696/#705)** — the *off-path* structural answer. Incremental Parquet
  projections carrying `__writetime`/`__deleted`/`__ttl` feed a lakehouse (Iceberg/Delta
  merge-on-read) that owns `count(*)`-by-footer and columnar scan. This is the **"other half"** the
  companion memo details: heavy historical reporting belongs on the export tier, not the live feed.
  *Exporter exists today (#682); the freshness/semantics tax is the cost.*
- **#1934 assembled-engine thesis** — the umbrella that makes #2037 viable *in-path* (byte-parity
  writer → stock Cassandra readers; ArrowMemtable as a CEP-11 plugin, a seam that actually exists).
  Changes nothing on its own; it's the strategic frame under which the structural items compose.
  *Longest horizon — research record, owner product decision pending.*

## The honest ceiling

A **row-oriented SSTable scan feed with cell-grain LWW/tombstone/TTL merge has a structural ceiling
vs columnar**, and it sits exactly where the matrix turns MARGINAL→NOT-YET. Three things a columnar
engine gets for free that this feed structurally cannot:
1. **Metadata-only `count(*)`** — Parquet reads a footer; we must merge every live cell across every
   generation (the 66.2 s / 1.94 M is that merge, and it is our *best case* for a full scan). [M/R]
2. **Column projection** — reading 2 of 40 columns still materializes the partition on our path;
   columnar reads 2 column chunks. [R]
3. **Selectivity ≠ less work** — a filtered scan feeds the same rows; the predicate saves Trino
   work, not feed work. [R]

The incremental levers (#2403, #2313, #2366, #2230) **push the feed rate and remove cliffs — they
raise the "how many rows under 5/15 min" thresholds and make concurrency clean — but they cannot
cross the row→columnar line.** They make cqlite-flight a *better operational gateway*; they do not
make it a warehouse.

**The item that actually answers the heavy-OLAP end is #2037 (ArrowMemtable) for the in-path future,
or the delta-export Parquet tier (#696/#705) for the off-path lakehouse today.** #941 (DataFusion
projection/pushdown) is the pragmatic middle — it lowers the constant enough to widen the MARGINAL
band, but it is not the columnar answer. Everything else on #2403 is *right work for the operational
tier* and *the wrong lever for the warehouse tier* — which is the whole point: **CQLite ships both
halves, and the honest position is to send bounded/fresh/operational analytics to the live feed and
heavy/historical/columnar analytics to the export tier**, not to try to make one path do both.

## Bottom line

Trino → cqlite-flight is **usable today, and good, for the analytic classes the Cassandra data model
already makes bounded** — operational dashboards over recent partition-scoped slices, partition-key
drill-downs, single-series time-series rollups — at 34 qps / p50 227 ms / ≤3 s freshness, which is a
capability a lakehouse structurally cannot match (zero-ETL, live). It is **size-gated (MARGINAL, up
to ~single-digit-million rows per ring per few minutes) for cross-partition aggregation and small×large
joins**, and **not yet the tool for large full-table reporting, `count(*)`-heavy dashboards, or
large×large joins** — those are the row-vs-columnar ceiling, and the roadmap's real answer for them
is #2037 / the export tier, not the incremental #2403 levers.
