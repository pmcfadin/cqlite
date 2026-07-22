# Phase 1-8 — Prior-art calibration of the achievable per-core scan envelope

**Date:** 2026-07-21 · **Status:** research (uncommitted) · **Author:** Phase-1 prior-art calibration agent

Purpose: give Phase 2 a defensible envelope so it can kill inflated multiplier claims. The anchor is
Phase 0 (`docs/research/phase0-scan-cost-breakdown-2026-07.md`): CQLite today does **~500-540 k rows/s
single-stream, warm, narrow-row** on an M1 Pro, and the CPU splits **merge channel coordination 49.9 %,
merge compute 32.5 %, parse/decode 9.7 %, row materialization 4.5 %, Arrow encode 1.0 %, malloc 17.6 %**.

**The single most important normalization rule in this document:** the headline "millions of rows/s/core"
numbers from ClickHouse/DuckDB/Velox are **columnar single-column SIMD aggregation** — they scan a few
bytes per row and reduce in place. CQLite's Flight pipeline **materializes every cell, reconciles a k-way
LSM merge with tombstones, and ships every row over the wire.** These are different workloads by 1-3
orders of magnitude. The only true structural analog is **ScyllaDB** (SSTable k-way merge with
tombstones), and even Scylla's billion-row number is *aggregation pushdown*, not row export. Always
normalize to **rows/s/core AND MB/s/core, with row width stated**, or the comparison is meaningless.

---

## 1. Calibration table (per-core prior art, with sources + hardware)

| System | Figure (normalized) | Workload / row width | Hardware | Columnar or row? Pushdown? | Source | Verified? |
|---|---|---|---|---|---|---|
| **ClickHouse** | **2-10 GB/s per core** | aggregation-heavy, compressed columns, few bytes/row touched | AMD EPYC Milan (c6a.4xlarge, 16 vCPU), AVX2 | Columnar SIMD, in-place aggregation | [pulse.support ClickBench KB](https://pulse.support/kb/clickhouse-benchmark) | Secondary (vendor-adjacent KB); order-of-magnitude only |
| **ClickHouse** | ~703 M rows/s single node ⇒ **~44 M rows/s/core** | 10 B-row aggregation, wide analytics table | 16-core node | Columnar SIMD aggregation | [clickhouse.com parallel-replicas blog](https://clickhouse.com/blog/clickhouse-parallel-replicas) | Vendor benchmark |
| **ClickHouse (cluster)** | 10.2 B rows/s / 192 GB/s over 9 nodes | 10 B-row aggregation | 9 ClickHouse Cloud nodes | Columnar, aggregation | same as above | Vendor benchmark |
| **DuckDB** | 200 M-row single-col aggregate in ~40 ms ⇒ order **~10²-10³ M rows/s/core** | one column read, single aggregate, ~4-8 B/row | MacBook Pro M1 (~8-10 cores) | Columnar vectorized (1024-2048 vector), SIMD | [duckdb.org benchmarks-over-time](https://duckdb.org/2024/06/26/benchmarks-over-time); [medium/ThinkingLoop](https://medium.com/@ThinkingLoop/parallel-query-processing-in-duckdb-85ecd1446176) | Interactive-chart figures not text-extractable → **UNVERIFIED exact ms**; magnitude corroborated by multiple secondary posts |
| **DuckDB (scaling)** | 7-8× on 8 cores vs 1 thread | Q1 aggregation, 120 M rows in <500 ms | 8-core laptop | Columnar, multi-threaded | [duckdb-in-depth/endjin](https://endjin.com/blog/2025/04/duckdb-in-depth-how-it-works-what-makes-it-fast) | Secondary |
| **Velox** | ~order-of-magnitude on CPU-bound TPC-H; 6-7× avg on Meta production | mixed OLAP | not stated in abstract | Columnar vectorized + adaptive | [Velox VLDB p3372](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf); [fb eng blog](https://engineering.fb.com/2023/03/09/open-source/velox-open-source-execution-engine/) | Paper PDF body not extractable → **absolute per-core UNVERIFIED**; speedups verified from abstract/blog |
| **ScyllaDB** ⭐ | 1 B rows/s cluster ⇒ **~428 k rows/s/core**; full-year cold **~417 k/core**; cached **~658 k/core** | aggregation scan (`count/sum/min/max`) over **narrow** temperature rows | ~83× n2.xlarge, 28 Xeon Gold 5120 @2.2 GHz/node, NVMe (≈2324 cores) | **Row/LSM SSTable k-way merge**, but **aggregation pushdown on-shard, no row transfer** | [scylladb.com 1B-rows blog](https://www.scylladb.com/2019/12/12/how-scylla-scaled-to-one-billion-rows-a-second/); [press release](https://www.scylladb.com/press-release/scylladb-smashes-performance-record/) | Vendor; node count 83 per fetch (some coverage says 40) → per-core **~400-450 k** is the defensible band |
| **ScyllaDB** | **~12,500 ops/s/core** | point reads/writes post-replication | shard-per-core (Seastar) | Row, point-op | [scylladb.com shard-per-core](https://www.scylladb.com/product/technology/shard-per-core-architecture/); [why-shard-per-core](https://www.scylladb.com/2024/10/21/why-scylladbs-shard-per-core-architecture-matters/) | Vendor rule-of-thumb |
| **Cassandra 5.0** | No clean per-core scan number; trie memtable = "30 % more data / same memory", faster reads | qualitative | — | Row/LSM, trie-indexed BTI | [Trie Memtables VLDB p3359](https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf); [C* 5.0 features blog](https://cassandra.apache.org/_/blog/Apache-Cassandra-5.0-Features-Trie-Memtables-and-Trie-Indexed-SSTables.html) | **No per-core rows/s figure found — UNVERIFIED** |
| **DataFusion** | **~950 k rows/s single core** (SSD-bound, historical floor) | scan, TPC-H era | single core, SSD-limited | Columnar (Arrow), but IO-bound | [andygrove.io 0.2.1 bench (2018)](https://andygrove.io/2018/03/datafusion-0.2.1-benchmark/) | Old (2018); treat as **floor**, not ceiling |
| **DataFusion** | fastest single-node ClickBench parquet; 14 GB / 100 M-ish rows | 43 aggregation/filter queries | c6a.4xlarge (16 core) | Columnar vectorized | [datafusion.apache.org fastest-parquet-clickbench](https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/) | Blog gives only relative "1.x" → **absolute rate UNVERIFIED** |
| **Arrow Flight** | **DoGet up to 6000 MB/s** multi-stream; **~2000 MB/s @16 streams remote (~125 MB/s/stream)**; **localhost single stream 2-3 GB/s (C++, no TLS)**; up to 10 GB/s localhost | Arrow RecordBatch transfer | Mellanox ConnectX-3/IB nodes; uses "up to half of system cores" | Columnar wire | [arxiv 2204.03032](https://arxiv.org/abs/2204.03032); [ACM 10.1145/3527199.3527264](https://dl.acm.org/doi/fullHtml/10.1145/3527199.3527264); [introducing-arrow-flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) | Paper; per-stream single-thread ceiling inferred, not stated → **per-stream UNVERIFIED beyond 2-3 GB/s localhost C++** |
| **Trino JDBC ingest** | **single JDBC conn ~32 MB/s / ~0.73 M rows/s**; 7× parallel ⇒ **~224 MB/s**; columnar HDFS scan ~10,300 MB/s | row-by-row JDBC deserialization into Pages | 4-node Trino, Oracle single-box source | **Row ingest = the bottleneck**; columnar = not | [starburst benchmarking-jdbc-bottleneck](https://www.starburst.io/blog/benchmarking-the-jdbc-bottleneck-in-trino/); [dangers-of-jdbc-bottleneck](https://www.starburst.io/blog/jdbc-trino-starburst/) | Vendor lab; figures explicit |

**Row-width warning (load-bearing):** the ClickHouse/DuckDB "10²-10³ M rows/s/core" figures touch
**4-8 bytes/row** (one column). CQLite Phase-0 rows are ~2 text columns, **~50 B packed on disk / ~300 B
on the Arrow wire** (150 MB/s ÷ 500 k rows/s). Scylla's temperature rows are narrow too but it never
serializes them out. A rows/s number without a width is not comparable; convert to MB/s/core before
trusting any cross-system multiplier.

---

## 2. Technique-transfer matrix (does it survive CQLite's constraints?)

Constraints CQLite must honor: Rust library; **k-way LSM merge with tombstone reconciliation** (read-time
reconciliation, not just physical dump); **parity oracles** (byte + query-semantic) that pin exact row
sets/order at a fixed `now`; **512 Mi memory ceiling**; embedded inside a Flight server, not a
thread-per-core process it owns end-to-end.

| Technique | Behind whose numbers | Transfers to CQLite? | Phase-0 stage it attacks | Constraint / caveat |
|---|---|---|---|---|
| **Run-length / no-overlap fast-path merge** (emit a whole SSTable run untouched when its key range doesn't overlap other inputs) | ScyllaDB LSM merge | **YES — highest structural fit** (same problem shape) | 4a merge compute (32.5 %) **and** 4b channel (49.9 %) | Legal only where key ranges are provably disjoint over the interval; tombstone/overlap regions still need per-row reconcile. Parity-safe: output set unchanged, just fewer heap/channel ops. |
| **Inline / single-thread merge for the few-SSTable case** (bypass thread-per-input + per-row `sync_channel`) | (Phase-0's own #1 lever; classic single-threaded LSM iterators) | **YES** | 4b channel coordination (**49.9 %**, the biggest single line) | The per-row cross-thread `send` IS the 50 %. For 4 SSTables the parallel-decode design loses. Parity-safe. |
| **Vectorized / batch-at-a-time through every operator** (1k+ row batches, not per-row) | ClickHouse, DuckDB, Velox | **PARTIAL** | 4b (batch the channel handoff), 4a (batch heap refill), 3 (batch materialize) | The merge is inherently row-ordered at cluster boundaries, but you can batch **within** a run and batch the channel message (N rows/send instead of 1). CQLite already batches only at the Arrow-encode tail (8192); the merge core is per-row. |
| **Arena / bump allocation** (per-batch arena for MergeEntry/RowKey/QueryRow) | Velox memory pools; ClickHouse arenas | **YES** | malloc (**17.6 %**) + materialization (4.5 %) | Rust `bumpalo`/reset-per-batch. Pure allocation strategy → parity-invariant. Watch the 512 Mi ceiling: bound arena to batch size. |
| **Late materialization** (defer PK `Vec` copy + full row assembly until survivors are chosen) | Velox, DuckDB, ClickHouse | **PARTIAL** | 3 materialization (4.5 %) + malloc + SipHash key-hash (4.5 %) | Reconciliation needs keys early but not full values; defer value assembly + `RowKey::new(pk.to_vec())` copies past the merge. Parity-safe if survivor selection is unchanged. |
| **Dictionary-preserving decode** (keep low-cardinality columns dict-encoded through to Arrow dictionary arrays) | ClickHouse, DuckDB, Velox, Arrow | **WEAK for this shape** | 2 decode (9.7 %), 5 Arrow encode (1.0 %) | Helps low-cardinality columns; the narrow text-PK `keyvalue` shape has high-cardinality keys → little benefit. Data-dependent; not a general lever. |
| **Thread-per-core / sharded (Seastar model)** | ScyllaDB, Seastar | **PARTIAL — big architecture change** | 4b (50 %) by removing cross-thread handoff entirely | CQLite is a lib inside a Flight/tokio server; it can't own the whole box the way Seastar does. A shard-per-core merge with inline (no channel) reconciliation captures most of the 4b win without a full runtime rewrite. High effort. |
| **SIMD decode** (vectorized vint/cell decode) | ClickHouse, Velox, DataFusion | **WEAK** | 2 decode (9.7 %) | vint/cell decode is branchy and serial; SIMD wins on bulk fixed-width columns, not variable-length Cassandra cells. Parse isn't the bottleneck anyway. Low priority. |

---

## 3. The sanity envelope (what Phase 2 can defend)

### 3a. Credible per-core ceiling for **row-pipeline** CQLite-shaped work (LSM merge + materialize + Arrow encode + ship)

**~150-600 k rows/s/core for narrow rows, with the current ~500 k single-stream number sitting mid-band —
but only the low end is safe under contention today.**

- The only structural analog, **ScyllaDB, sustains ~400-450 k rows/s/core** on an LSM SSTable k-way merge
  of narrow rows — **and that is with aggregation pushdown (no row materialization or transfer)**. Row
  export is strictly heavier than Scylla's on-shard reduce, so Scylla's per-core rate is a **generous
  upper bound**, not a floor, for a row-export pipeline.
- CQLite's own Phase-0 single-stream ~500 k rows/s/core looks competitive with Scylla — **but ~68 % of
  that core's budget is waste** (49.9 % channel coordination + ~18 % malloc). The *useful* compute
  (decode + reconcile + materialize + encode ≈ 32 %) is doing ~500 k rows/s on a third of a core, which
  is why the levers in §2 have real headroom. Removing the per-row channel and the per-row allocations is
  what converts single-stream throughput into **linear multi-core scaling**.

### 3b. Does prior art support **600 k rows/s/pod on 4 vCPU (~150 k rows/s/core) on a ROW pipeline?**

**YES — credible, and conservative — but it is a post-fix target, not a current-architecture given.**

- 150 k rows/s/core is **~⅓ of Scylla's per-core LSM rate** and **~⅓ of CQLite's own measured single-stream
  rate**. There is nothing exotic about it *per core*.
- The risk is **not** the per-core ceiling; it is **scaling to 4 concurrent cores**. Today's thread-per-input
  design already spends ~50 % of a single stream's CPU parking on a cap-256 channel; four concurrent scans
  contend harder, so naive 4× fan-out will **not** reach 600 k without first landing the §2 levers
  (no-overlap fast-path, inline/batched merge, arena alloc). Claim it as an **engineering target gated on
  the merge-coordination fix**, not a property of the shipped code.

### 3c. Does prior art support **≥250-350 MB/s/worker ingest into a JVM (Trino page building)?**

**YES via a columnar/Arrow path; NO via row-oriented JDBC ingest — and this is a load-bearing distinction.**

- **JDBC row ingest ceiling is ~32 MB/s / ~0.73 M rows/s per connection** (Starburst lab). Parallel physical
  partitions reach only **~224 MB/s** and plateau (source-side limited). A per-worker 250-350 MB/s target
  **cannot** be hit through a row-by-row JDBC deserialize-into-Pages path — that is the documented Trino
  bottleneck the whole exercise exists to escape.
- The **columnar/Arrow path clears it with room to spare**: Trino columnar (HDFS) scans hit ~10,000 MB/s;
  **Arrow Flight single-stream localhost is 2-3 GB/s (C++, no TLS)** and multi-stream up to 6 GB/s. 250-350
  MB/s/worker is **~10-15 %** of the single-stream Arrow ceiling — comfortably credible **provided ingest is
  Arrow batches, not JDBC rows**, and provided the Flight producer can actually *fill* the wire (Phase-0
  shows CQLite's producer is merge/alloc-bound long before it saturates a 2-3 GB/s wire, so the worker-side
  JVM page building is **not** the binding constraint — the CQLite producer is).

### 3d. Columnar CQLite-shaped work

Prior art supports **millions of rows/s/core** — **but only for pushdown aggregation / single-column scans**,
never for full-row materialization and transfer. A CQLite Flight `do_get` that returns whole rows **cannot
inherit those numbers**. Columnar-grade rates apply only to a hypothetical CQLite path that pushes
aggregation/projection down and never materializes full rows (that path does not exist today).

---

## Final packet

**Envelope (defensible bands):**
- **Row-pipeline CQLite-shaped work (LSM merge + materialize + Arrow encode + ship), narrow rows:**
  credible **150-450 k rows/s/core**; Scylla's ~400-450 k/core LSM rate is a generous *upper* bound (it
  aggregates on-shard, never ships rows). CQLite's measured ~500 k single-stream is real but ~⅔ waste.
- **600 k rows/s/pod on 4 vCPU (150 k rows/s/core):** credible and conservative **per core**; the binding
  risk is 4-way concurrent scaling, which requires the merge-coordination fix first. A post-fix target.
- **Columnar CQLite-shaped work:** millions of rows/s/core **only** with aggregation/projection pushdown
  (no full-row export). Not applicable to today's row-returning `do_get`.
- **Ingest MB/s/worker into the JVM:** **250-350 MB/s/worker is credible via Arrow batches** (Arrow Flight
  single-stream localhost 2-3 GB/s; the CQLite producer, not the JVM page builder, is the real limiter).
  **Not achievable via JDBC row ingest** (~32 MB/s/conn, ~224 MB/s parallel plateau).

**5 most transferable techniques (ranked by impact × fit):**
1. **Inline / batched merge that eliminates the per-row cross-thread `sync_channel`** — attacks stage 4b
   (**49.9 %** of single-stream CPU), the biggest single line. Parity-safe. (Phase-0's own #1 lever.)
2. **Run-length / no-overlap fast-path merge (Scylla model)** — emit disjoint SSTable runs without per-row
   heap/reconcile ops; attacks 4a (32.5 %) + 4b. Highest *structural* fit — same LSM-merge problem shape.
3. **Vectorized batch-at-a-time through the merge core** (N rows/channel-message, batched heap refill) —
   attacks 4b/4a/3; CQLite currently batches only at the Arrow tail, not the merge.
4. **Arena / bump allocation per batch** — attacks the **17.6 % malloc** + per-row `RowKey`/`QueryRow`
   copies; parity-invariant; bound to batch size for the 512 Mi ceiling.
5. **Late materialization + cheaper PK hashing** (defer `pk.to_vec()` copy + value assembly past survivor
   selection; drop the default SipHash `HashMap` hasher) — attacks stage 3 (4.5 %) + SipHash (4.5 %) + malloc.

**Red flags for common multiplier claims:**
- **"Columnar engines do 100 M-1 B rows/s/core, so we can multiply by 10-100×."** Category error: those are
  single-column SIMD *aggregation* numbers (4-8 B/row, no materialization, no transfer). CQLite ships every
  cell. Do not import them into a row-export pipeline.
- **"Scylla does 1 B rows/s, LSM is LSM."** Scylla's number is **aggregation pushdown across ~2324 cores
  (~428 k/core) with zero row materialization or cross-node row transfer.** It is an *upper bound* for a
  heavier row-export workload, not a matchable target.
- **"600 k rows/s/pod is a given."** Credible *per core*, but gated on removing the per-row channel
  coordination that today eats ~50 % of a single stream; naive 4× fan-out on the current design will not
  reach it. Present as a post-fix target.
- **"250-350 MB/s/worker ingest is fine."** True for **Arrow** ingest only. Over **JDBC row ingest it is
  physically off the table** (~32 MB/s/conn). Any claim that doesn't name the transport is unfalsifiable.
- **Row-width laundering:** comparing CQLite's narrow-row rows/s to a competitor's different row width (or
  citing rows/s with no width). Always co-report MB/s/core.
- **Local-to-field extrapolation:** never multiply Phase-0's warm + uncompressed + RF=1 + loopback
  single-stream figure by pod count to predict a compressed, RF=3, through-Trino field number.

**Verification flags:** DuckDB exact-ms (interactive charts, not text), Velox absolute per-core (paper body
not extractable), DataFusion ClickBench absolute rate (blog gives only relative "1.x"), Cassandra per-core
scan rate (no figure found), Arrow Flight per-stream single-thread ceiling beyond the 2-3 GB/s localhost C++
point, and Scylla's exact node count (83 per the blog fetch vs 40 in some coverage) are all flagged
UNVERIFIED / order-of-magnitude in §1. ClickHouse "2-10 GB/s/core" is a vendor-adjacent rule of thumb.
