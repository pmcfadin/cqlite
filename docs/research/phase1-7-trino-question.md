# Phase 1-7 — Alternative engines: the Trino question

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Author:** Phase-1 agent 7/8 (strategic engine comparison)

Read-only strategic analysis. Companion to agent 5's connector-mechanics decomposition (same code,
different altitude: agent 5 owns *how* to fix the connector; this owns *whether fixing it is the
right bet* vs switching engines). Anchored to Phase 0 (`phase0-scan-cost-breakdown-2026-07.md`), the
throughput backlog inventory (`throughput-backlog-inventory-2026-07.md`), the ratified performance
goals (`performance-goals-2026-07.md`, branch `research-parquet-comparison`), the class-marks memo
(`perf-class-marks-2026-07.md`, same branch), and the #941 Design-A packet + decision brief
(`docs/architecture/`).

---

## 0. The one number that reframes the whole question

**Trino is not a 30× tax on scan throughput. The row-at-a-time feed is.**

- Field **server-direct** full-ring scan (round-11b, #2367): **~29 k rows/s/ring**.
- Field **through-Trino** full-ring scan (B3, round-12): **1.94 M rows / 61 s over 3 pods ≈ 31.8 k
  rows/s aggregate ≈ 10.6 k rows/s/pod.**

Through-Trino aggregate throughput (~32 k rows/s) is *roughly the same as* server-direct ring
throughput (~29 k rows/s). Trino is **not** eating an order of magnitude off the scan rate — it is
handing the same row-engine feed to more cores and getting back approximately what the feed can
produce. The Iceberg/Hive connectors reach **GB/s per worker** through the *same Trino* because they
feed it **vectorized columnar Parquet/ORC pages** with predicate + partition + row-group pruning
([LakeOps], [Trino docs]). Our connector diverges by feeding Trino **one row at a time, materialized
then re-encoded into Arrow builders** — Trino per se is fast; our feed is slow.

Two distinct Trino costs must be separated, because they have opposite fixability:

1. **Per-query latency floor ≈ 0.2–2 s wall** (planning 20–50 ms + split sourcing + scheduling +
   a collection stage), paid by *every* connector, Iceberg included ([class-marks], [Trino #10971],
   [Trino #15252]). **Structural. Not fixable in our connector.** It gates point-read / interactive
   latency (B1) and makes Pinot-class 70–100 ms p99 *unreachable through Trino by construction*.
2. **Scan throughput tax ≈ small.** The 61 s is dominated by the row-engine feed ceiling
   (~10 k rows/s/pod), **not** by Trino overhead. **Fixable** — in the server (constant factors,
   parallelism) and the connector (split granularity, pod balance, backpressure relief).

Everything below follows from that split.

---

## 1. Where the 61 s actually goes (B3 decomposition)

| Contributor | ~share of the 61 s | Fixable where | Evidence |
|---|---|---|---|
| Trino coordinator floor (plan+schedule+collect) | ~0.2–2 s (≤3 %) | **Trino-structural** — not ours | [class-marks], [Trino #10971] |
| Split scheduling of many 1-per-vnode splits | small but real | connector (#2680 balance, sub-splitting) | `CqliteFlightSplitManager.java:28` "one split per token range" |
| **Row-engine scan feed (decode → merge → materialize → Arrow)** | **the bulk (~90 %+)** | **server + connector parallelism** | Phase 0 §3; ~10 k rows/s/pod |
| Egress channel backpressure (consumer drain vs producer) | material under load | server (#2600 shipped / #2765) | R12: 3,505 rows queued at merge-egress |
| Arrow-flight transport | ~0 % | n/a (µs-class, ~1 GB/s) | [class-marks] #2/#5 |

The scan feed is limited by two things Phase 0 named for the *single stream* (per-row cross-thread
channel handoff = 55 % kernel, allocator = 18 %, own compute = 22 %) **and** by the field pods not
using their cores: **1 split per vnode range, pinned to 1 replica, scanned single-threaded**
(`CqliteFlightSplitManager` produces one split per read-replica range). An i4i.xlarge is 4 vCPU; a
per-pod feed of ~10 k rows/s while the pod has 4 cores is the tell that the scan is running on ~1 of
them and stalling on egress. **The dominant lever is parallelism-to-all-cores, then constant
factors.**

---

## 2. Target arithmetic — what each B-goal actually demands

Baseline: 1.94 M rows / 61 s / 3 pods; B2 34–39 qps; B1 p50 227 / p99 366 ms.

| Goal | Baseline | Target | Speedup needed | Per-pod feed implied (3 pods) |
|---|---|---|---|---|
| B1 warm interactive | 227 / 366 ms | ≤300 / ≤500 ms | already at floor | — (gateway-bound) |
| B2 concurrency | 34–39 qps @≤32thr | **≥100 qps @32thr** | ~2.6–3× | needs balanced pods + warm p50 ≤~0.3 s |
| **B3 Stage 1** | 61 s | **≤10 s** | **~6.1×** | ~10 k → **~65 k rows/s/pod** |
| **B3 Stage 2** | 61 s | **≤3 s** | **~20×** | ~10 k → **~215 k rows/s/pod ≈ 54 k rows/s/core** |

The two B3 rungs land in different worlds:

- **65 k rows/s/pod** (Stage 1) is reachable by using all 4 cores (4× from parallelism) plus a modest
  constant-factor win (~1.5×) from Stage-1 levers (egress #2600, zero-copy #1644). The #941 decision
  brief's own independent estimate — Stage-1 levers alone → **66 s → 15–25 s** — corroborates that
  parallelism is the missing multiplier: constant factors get ~2.5–4×, cores supply the rest.
- **54 k rows/s/core** (Stage 2) at 4 vCPU means **~1.6 µs/row end-to-end including decode, merge,
  reconcile, and Arrow encode.** Phase 0 puts single-stream own-compute at ~22 % of a ~500 k-rows/s
  ceiling on an *unloaded warm M1* with *2 narrow columns* — real field rows (compressed, wider,
  reconciliation overlap) are far heavier per row. **1.6 µs/row is vectorized-execution territory, not
  row-at-a-time territory.** This is the same conclusion the Design-A packet and the ladder math
  reached ("Stage 2 is not row-engine territory").

---

## 3. The four options, modeled honestly

For each: projected B3/B2, what we lose, engineering cost (S/M/L in issue-count terms), migration risk.

### Option 1 — Fix the connector + server, keep Trino (baseline)

Assume the agent-5-class fixes land: weight-balanced sub-splits so every pod core scans (#2680, but
see risk), adaptive egress budget (#2600 shipped / #2765 impl), zero-copy value extraction (#1644),
partition-materialization bounds (#2230/#2423 shipped). None of these touch Trino; they change what
the connector feeds it and how many streams feed it.

- **Floor Trino imposes:** the **0.2–2 s per-query coordinator floor** (hurts B1/point reads, not
  scans) and **coordinator scaling to ~tens of concurrent queries** ([class-marks]). Neither bounds a
  3-pod full scan.
- **Prior art that our feed diverges from:** Iceberg/Hive connectors hit GB/s per worker via
  vectorized columnar readers + pushdown pruning ([LakeOps], [Trino episode 40]). Our divergence is
  the row-at-a-time Arrow feed (Phase 0 §3: per-row channel handoff + per-row alloc) and single-stream
  per-split scan — **both in our code, both addressable.**
- **Projected B3:** ~61 s → **~10–20 s** (parallelism 4× + constants ~1.5–2×). Stage 1 **≤10 s
  reachable but at the edge** — it needs full-core parallelism *and* the constant-factor wins to both
  land, not one or the other.
- **Projected B2:** ~39 → **~80–110 qps** once pod skew is removed (#2680 targets 2–4× skew) and warm
  p50 sits near the B1 floor (~0.3 s → 32 threads / 0.3 s ≈ 100 qps ceiling per the floor arithmetic).
  **≥100 qps reachable.**
- **What we lose:** nothing. Keeps JDBC/BI (Tableau/PowerBI), federation, MPP scheduling, fault
  tolerance, all easy-db-lab tooling.
- **Cost:** **M** — mostly already-filed 0.16 issues (#2680, #2765, #1644, #2371–#2377 coverage). No
  new architecture.
- **Risk:** **medium** — #2680's `sub-splits-per-range=4` default already caused a **live P0 (#2782,
  LIMIT hang)**: sub-split + LIMIT + Trino early-termination ⇒ a DoGet stream not drained. The
  parallelism lever is real *and* the exact one currently on fire. Flight↔Trino E2E is not yet a
  `required` check (#2792).

### Option 2 — DataFusion as the SQL layer (#941 + #2037)

DataFusion is the **fastest single-node Parquet engine** (ClickBench, ahead of DuckDB/ClickHouse,
[InfluxData]/[DataFusion blog]) and vectorized. But it is **single-node, embeddable — no distributed
MPP** ([Slashdot compare]). The Design-A packet reflects this honestly: DataFusion is a co-located
`TableProvider` that **still pulls Arrow from `cqlite-flight` per partition**; **Trino stays the MPP
owner**. So Option 2 is *not* "replace Trino" — it is "add a second, vectorized SQL surface."

- **The trap:** DataFusion-over-Flight **does not fix B3 by itself.** If the feed is still the row
  engine, DataFusion just consumes the same ~10 k rows/s/pod. The vectorization win only materializes
  when the **feed becomes columnar** — i.e. #2037 (ArrowMemtable / vectorized decode). **Swapping the
  SQL engine on top of a row feed is a no-op for scan throughput.** This is the single most important
  finding for this option: the bottleneck is the feed, not the SQL tier.
- **Projected B3:** with a columnar feed (#2037) + vectorized exec, **≤3 s is the only credible path
  to Stage 2.** Without the columnar feed, no better than Option 1.
- **Projected B2:** DataFusion doesn't change the Trino path's concurrency; if used as a *replacement*
  coordinator it loses distribution entirely (single-node). Neutral-to-worse for B2.
- **What we lose if DataFusion *replaces* Trino as the gateway:** MPP scheduling, fault tolerance,
  cross-catalog **federation** (joins to other sources), the mature **JDBC/BI ecosystem** Trino gives
  us (Tableau/PowerBI), and the existing easy-db-lab + trino-connector tooling. As an *additive*
  embedded/programmatic surface (Design-A's actual proposal), we lose none of that but take on a heavy
  dependency.
- **Cost:** **L** — #941 epic children #1905–#1914 (10 issues, all unstarted P3) **plus** #2037
  ArrowMemtable (exploration epic, WS1–9 mostly unfiled) for the feed. Arrow-version skew (DataFusion
  ahead of repo's Arrow 53) is a standing integration tax.
- **Risk:** **high** — heavy dep landing while the row path churns under it; moving baseline (the
  #941 brief's explicit reason for Option-C-spike-first); the columnar-feed half (#2037) is itself an
  owner-gated multi-release exploration, not a scheduled build.

### Option 3 — Flight SQL direct (no SQL middle tier for programmatic consumers)

Expose Flight SQL (or the existing Flight `do_get` ticket surface) directly to **programmatic
consumers** (pandas/ADBC, Spark, Python, Rust) that don't need Trino's SQL gateway.

- **Which workloads bypass SQL:** bulk export, ETL feeds, notebook/dataframe pulls, ML feature
  extraction — anything that wants *rows fast* and can express its predicate as a ticket. These hit
  the **server-direct ceiling** (A-lane: ~500 k rows/s/stream local, ~29 k rows/s/ring field today,
  tens-of-ms warm point reads once cold-parse #2385/#2412 is fully paid down) and **escape the
  0.2–2 s coordinator floor entirely** ([class-marks] engine-vs-gateway separation).
- **Which cannot:** BI tools. Flight SQL JDBC exists as a universal driver ([Dremio]) but **Tableau
  does not support it** and recommends vendor drivers ([Dremio Tableau note]); Power BI/DBeaver
  support is uneven. So Flight SQL does not retire Trino for the BI seat.
- **Cost of a second query surface:** Flight **SQL** (prepared statements, catalog metadata, SQL
  parse/plan) is more than the current ticket API — a SQL front end is, effectively, DataFusion again
  or a hand-rolled CQL-subset planner. The *ticket-level* bulk API (no SQL) is much cheaper and already
  ~exists.
- **Projected B3/B2:** **does not move them** — B3/B2 are through-Trino by definition. It makes them
  *partly moot* for the export/programmatic slice by giving those users the A-lane numbers instead.
- **Cost:** **M** (ticket-level bulk path, mostly present) to **L** (full Flight SQL surface + planner).
- **Risk:** low if additive; the standing cost is maintaining a second surface's semantics/parity.

### Option 4 — Hybrid: Trino for SQL + a Flight-native bulk-path API

Keep Trino + connector fixes (Option 1) for ad-hoc SQL/BI; **add a Flight-native bulk-export path**
(the cheap end of Option 3) feeding pandas/Spark/Arrow consumers directly at server-direct rates.

- This is the honest reading of the *ratified goal tables themselves*: **A4 (scan rows/s/pod) is a
  server-direct goal already**; **B3 (through-Trino full scan) stays Trino-bound.** The bulk path
  *is* how A4 gets delivered to real consumers.
- **Does it satisfy A4 while B3 stays Trino-bound?** Yes — A4 is server-direct-bound regardless of
  Trino, so the bulk path delivers it directly; B3 remains a separate through-SQL number governed by
  Option 1.
- **Is B3 ≤10 s achievable with connector fixes alone?** **Yes for Stage 1** (Option 1 arithmetic).
  **No for Stage 2 ≤3 s** — that needs Option 2's columnar feed.
- **What we lose:** nothing; this is strictly additive and matches the ladder's own sequencing.
- **Cost:** **M** (Option 1 issues, already filed) **+ S–M** (ticket-level bulk path).
- **Risk:** low-medium (inherits Option 1's #2782 fire; the bulk path itself is low-risk).

### Option summary table

| Option | Projected B3 | Projected B2 | What we lose | Cost | Migration risk |
|---|---|---|---|---|---|
| **1. Fix connector, keep Trino** | ~10–20 s (Stage 1 ≤10 s at the edge) | ~80–110 qps (**≥100 reachable**) | nothing | **M** | medium (#2782 live P0 on the exact parallelism lever) |
| **2. DataFusion SQL layer** | ≤3 s **only with columnar feed #2037**; else = Option 1 | neutral / worse if it replaces Trino (single-node) | federation, JDBC/BI, MPP, tooling (if replacing) | **L** | high (heavy dep, moving baseline, Arrow skew) |
| **3. Flight SQL direct** | unchanged (bypasses, doesn't fix) | unchanged | nothing (additive) | **M–L** | low (2nd-surface maintenance) |
| **4. Hybrid (Trino SQL + Flight bulk)** | Stage 1 ≤10 s via Opt 1; A4 delivered directly | ≥100 via Opt 1 | nothing | **M (+S)** | low-medium |

---

## 4. Per-target verdict — required vs nice-to-have

| Target | Verdict | Required option | Why |
|---|---|---|---|
| **B1 warm ≤300/≤500 ms** | met at baseline (227/366) | Option 1 (hold the floor) | gateway-bound; already at Trino's coordinator floor. Below ~200 ms is unreachable *through Trino* — go server-direct (Opt 3/4) for tens-of-ms. |
| **B2 ≥100 qps @32thr** | **reachable** | **Option 1 REQUIRED** | pod-skew balance (#2680) + egress relief (#2600/#2765) + warm p50 ≤~0.3 s. Trino coordinator handles tens of concurrent queries — not structurally blocking at this scale. |
| **B3 Stage 1 ≤10 s** | **reachable, at the edge** | **Option 1 REQUIRED & SUFFICIENT** | needs full-core parallelism (~4×) **and** constant-factor wins (~1.5–2×) to *both* land. Trino is not the limiter; the row feed and split granularity are. Option 4's bulk path makes it moot for export consumers. |
| **B3 Stage 2 ≤3 s** | **NOT credible through Trino with a row feed** | **Option 2 (#941 vectorized) + columnar feed (#2037)** | ~20× / ~1.6 µs/row = vectorized territory. **Limiting factor named:** per-row materialization + per-row Arrow-builder + single-threaded merge-coordinator reconcile. A row-at-a-time engine cannot hit 54 k rows/s/core; no connector fix closes this. |
| **A1–A4 (server-direct)** | server-direct-bound | Option 3/4 (Flight bulk path) | already independent of Trino; the coordinator floor is the reason to offer a non-SQL path at all. |

---

## 5. No-optimism-theater statement

**Stage-2 B3 ≤3 s through Trino is not credible with the current architecture, and it is not credible
with a connector fix.** The limiting factor is not Trino and not the wire — it is that a
**row-at-a-time feed (decode → reconcile → per-row materialize → per-row Arrow builder → per-row
cross-thread channel handoff)** cannot sustain the ~1.6 µs/row the target implies. Phase 0 measured
that even a warm, uncompressed, 2-column single stream spends 55 % of CPU in per-row channel park/wake
and 18 % in the allocator; real field rows are heavier. Reaching ≤3 s requires **columnar execution
(#941 DataFusion) fed by a columnar source (#2037 ArrowMemtable / vectorized decode)** — two
owner-gated exploration tracks, a **multi-release bet**, not a 0.16/0.17 connector fix. Anyone
promising ≤3 s through-Trino from connector work alone is mis-reading the feed as the SQL tier.

---

## 6. Recommendation

**Adopt Option 4 (Hybrid) now; bank the #941 spike; do not promote Option 2 yet.**

1. **Now (0.16/0.17), Option 1 + the bulk path (Option 4):** land the already-filed connector+server
   fixes (#2680 weight-balanced splits — *after* #2782 is resolved and #2792 makes the E2E lane
   `required`), #2600/#2765 egress budget, #1644 zero-copy). This delivers **B2 ≥100 qps** and
   **B3 Stage 1 ≤10 s** (at the edge — measure, don't assume). Add the **ticket-level Flight bulk
   path** so A4/A-lane consumers (pandas/Spark/ADBC) get server-direct rates and skip the coordinator
   floor. Least regret, matches the ladder's own sequencing, loses nothing.

2. **Keep the #941 spike (#2605) banked, decide on data.** The decision brief's trigger stands: if a
   field round shows Stage-1 landing short of **~30 k rows/s/pod**, promote #941; if it clears that,
   #941 targets 0.17. **But correct the brief's framing in one respect:** promoting #941 *without*
   the columnar feed (#2037) will not move B3 — sequence #2037's vectorized-decode spike (WS7 #2043)
   *ahead of or with* any #941 promotion, or the DataFusion bet buys nothing on scan throughput.

3. **Do not treat DataFusion as a Trino replacement.** It is single-node; replacing Trino forfeits
   federation, the JDBC/BI seat (Tableau has no Flight SQL support), MPP, and all existing tooling.
   Its role is *additive* vectorized execution over a *columnar feed* — valuable only for Stage 2, and
   only in tandem with #2037.

**Bottom line for the program:** the through-Trino tax is a *latency floor* (structural, ~0.2–2 s,
matters for point/interactive → answer is go server-direct) plus a *throughput ceiling* that is **our
row feed, not Trino** (fixable → Option 1 gets Stage 1 + B2). Trino stays. The genuine engine question
is **not** Trino-vs-DataFusion; it is **row-feed-vs-columnar-feed** — and that question is forced only
at Stage 2, on data, when Stage 1 saturates.

---

### Sources
- Internal: `phase0-scan-cost-breakdown-2026-07.md`, `throughput-backlog-inventory-2026-07.md`,
  `performance-goals-2026-07.md` + `perf-class-marks-2026-07.md` (branch `research-parquet-comparison`),
  `docs/architecture/issue-941-design-a-colocated-flight-provider.md`,
  `docs/architecture/941-datafusion-decision-brief-2026-07.md`,
  `docs/architecture/trino-flight-read-path-audit-2026-07-08.md`,
  `trino-connector/.../CqliteFlightSplitManager.java`.
- Trino connector scan prior art: [LakeOps Trino-Iceberg optimization](https://lakeops.dev/blog/trino-iceberg-optimization),
  [Trino episode 40 (Iceberg)](https://trino.io/episodes/40.html),
  [Iceberg connector docs](https://trino.io/docs/current/connector/iceberg.html).
- Trino coordinator floor / scheduling: [Trino #10971 split-sourcing metrics](https://github.com/trinodb/trino/issues/10971),
  [Trino #15252 scheduling time](https://github.com/trinodb/trino/discussions/15252),
  [Shopify faster Trino](https://shopify.engineering/faster-trino-query-execution-infrastructure).
- DataFusion: [DataFusion fastest single-node Parquet (ClickBench)](https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/),
  [InfluxData writeup](https://www.influxdata.com/blog/apache-datafusion-fastest-single-node-querying-engine/),
  [DataFusion vs Trino compare](https://slashdot.org/software/comparison/Apache-DataFusion-vs-Trino/).
- Flight SQL / BI: [Dremio Arrow Flight SQL universal JDBC](https://www.dremio.com/blog/arrow-flight-sql-a-universal-jdbc-driver/),
  [Dremio: will Flight SQL replace ODBC/JDBC (Tableau limitation)](https://www.dremio.com/blog/will-apache-arrow-flight-sql-replace-odbc-and-jdbc-for-analytics-bi-workloads/).
