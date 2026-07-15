# Performance class-marks — what "in-class" means per analytic-serving shape (2026-07)

**Status:** Research memo. Harvests HARD published numbers that define in-class performance for each
analytic-serving shape, to set numeric latency/throughput goals for the Trino→cqlite-flight path and
the server-direct Flight path. Every number is cited (source + hardware/scale context); read them as
order-of-magnitude class-marks, NOT normalized head-to-heads (hardware differs per source).
**Companion to:** `htap-positioning-2026-07-landscape.md` (taxonomy — not duplicated here),
`parquet-backend-comparison-2026-07.md`. **cqlite baseline:** round-11b field run 2026-07-15 (#2367):
warm ~34 qps interactive point/LIMIT; ~29k rows/s/ring full-ring scan; point-read p50 ~2.1s cold-parse-bound.

## The 3 numbers most binding for goal-setting

1. **Trino coordinator floor ≈ 0.2–2 s wall-clock on ANY connector.** Planning alone is 20–50 ms for
   simple queries; split sourcing + scheduling + a collection stage push real end-to-end minima into
   the hundreds-of-ms-to-low-seconds band even for tiny results. **This is a hard floor on the
   through-Trino path** — our server-direct Flight path is the only way under it. Our round-11b ~2.1 s
   point-read sits right at this floor, so point-lookup latency is gateway-bound, not engine-bound.
2. **Arrow Flight transport is effectively free vs our floor:** ~1 GB/s single stream (up to ~10 GB/s
   at 16 parallel streams), small-result latency mean ~174 µs / p50 ~95 µs. The transport does NOT bound
   us; SSTable read + row materialization does. Server-direct ceiling is set by cqlite, not the wire.
3. **Keyed point lookup in-class = single-digit ms (OLTP floor); user-facing OLAP p99 = ~70–100 ms
   (Pinot prod).** Anything in seconds is out-of-class for interactive point/dashboard serving — the
   honest target for cqlite-flight server-direct is "tens of ms once warm," gated on killing cold-parse.

## Class-mark table (shape × system × number × context)

| Shape | System | Published number | Hardware / scale context |
|---|---|---|---|
| **1. Keyed point lookup** | Cassandra/DynamoDB (OLTP floor) | single-digit ms p99 | the reference floor; native OLTP point read |
| | Apache Pinot (prod, keyed) | p99 **70 ms** (Stripe BF/CM); **80–100 ms** p99 @ multiple-1000s qps (LinkedIn) | 300M+ txns; LinkedIn also 200k qps @ 100 ms p95 |
| | Apache Pinot (Uber UberEats) | **<100 ms p99** @ 100s qps | 10B+ row tables, Neutrino 500M+ queries/day |
| | ClickHouse (point SELECT by PK) | "meaningfully slower for single-row; not designed for it" | sparse PK index, no B-tree — NOT a point-lookup engine |
| | Trino-on-Parquet (keyed) | bounded by coordinator floor (0.2–2 s) | keyed lookup ≠ Trino's lane; floor dominates |
| | Rockset (converged index, historical) | ms-class point + search on RocksDB-on-S3 | separate managed cloud store (now retired) |
| **2. Interactive filtered aggregation** | Apache Pinot (star-tree) | **6.3 s → 15 ms** (99.76% cut) prod; **3,494 qps on 4 vCPU** | precomputed aggregation paths matching query shape |
| | Apache Pinot (high-concurrency) | **40k–100k+ qps @ sub-second p99** full analytical | demonstrated concurrency footprint |
| | ClickHouse (dashboard slice) | **2–10 GB/s per core** aggregation-heavy | depends on compression + filter selectivity |
| | Apache Druid | "sub-second queries at scale and under load" (SSB) | p95 highly tuning-dependent (tiering/segment sizing) |
| | Trino (interactive band) | seconds, floored at coordinator 0.2–2 s | every connector pays the same floor |
| **3. Full-scan throughput** | ClickHouse | **~200M rows/s/core** (indexed MergeTree); GROUP BY 10B rows: 127M/1c → 998M/8c → 5.5B/64c | modern hardware, vectorized SIMD |
| | TiFlash | **1.1 GiB/s/node** stable layer (111 MB/s delta layer) | columnar Raft-learner replica |
| | DuckDB | "GB/s on a laptop" single-node Parquet scan | local, columnar-native |
| | Spark-Cassandra-connector | **~600k rows/s/node** (6M/s @ 10 nodes RF3); ~185k rows/s count(*) | the ancestor — through the live serving path |
| | Trino-Parquet | scan bounded by worker CPU/mem; columnar pushdown | no single published MB/s/worker constant found |
| | **cqlite-flight (ours)** | **~29k rows/s/ring** full-ring (round-11b) | row-feed into Trino, NOT columnar — 1–2 orders below columnar-native |
| **4. Concurrency footprint** | Apache Pinot | **100k+ qps** class; LinkedIn 200k qps @ 100 ms p95 | thousands of qps is routine |
| | ClickHouse | tens of thousands qps @ <100 concurrent; `max_concurrent_queries` default 100→1000; sizing ~2×cores | concurrency ≠ qps; light queries mostly network-wait |
| | Trino | tens of concurrent queries/coordinator; lower concurrency cuts coordinator lock contention | coordinator is the scaling bottleneck |
| | **cqlite-flight (ours)** | warm **~34 qps** interactive; 3/3-pod rotation p50 2.9 s / p99 10 s | round-10b, connector 0.14.2 |
| **5. Flight/gRPC transport floor** | Arrow Flight | **~1 GB/s single stream**, up to **~10 GB/s @ 16 streams**; DoGet ~6000 MB/s | localhost/RDMA; 19.1–27.8M records/s (Java) |
| | Arrow Flight (small-result latency) | mean **174 µs**, p50 **95 µs**, p95 **546 µs** | 262 KB batches @ 79.6k batches/s |

Sources: point-lookup/aggregation/concurrency — [StarTree p99 SLA](https://startree.ai/resources/achieving-99th-percentile-latency-sla-using-apache-pinot),
[Uber low-latency Pinot](https://www.uber.com/us/en/blog/pinot-for-low-latency/),
[StarTree star-tree part 2](https://startree.ai/resources/star-tree-indexes-in-apache-pinot-part-2-understanding-the-impact-during-high-concurrency/);
ClickHouse scan/concurrency — [ClickHouse benchmark KB](https://pulse.support/kb/clickhouse-benchmark),
[parallel replicas 100B GROUP BY](https://clickhouse.com/blog/clickhouse-parallel-replicas),
[ClickHouse concurrency sizing](https://clickhouse.com/resources/engineering/high-concurrency-sizing-user-analytics),
[max_concurrent_queries 100→1000 PR](https://github.com/ClickHouse/ClickHouse/pull/53285);
scan — [TiFlash tuning](https://docs.pingcap.com/tidb/stable/tiflash-performance-tuning-methods/),
[DuckDB benchmarks](https://duckdb.org/docs/current/guides/performance/benchmarks),
[Spark-Cassandra benchmark thread](https://groups.google.com/a/lists.datastax.com/g/spark-connector-user/c/UJUYZs_2qXI);
Druid — [Apache Druid](https://druid.apache.org/), [Imply sub-second](https://imply.io/blog/learn-how-to-achieve-sub-second-responses-with-apache-druid/);
Trino floor/concurrency — [Presto low-latency #1141](https://github.com/prestosql/presto/issues/1141),
[Trino split-sourcing #10971](https://github.com/trinodb/trino/issues/10971),
[Shopify faster Trino](https://shopify.engineering/faster-trino-query-execution-infrastructure),
[Trino query-management props](https://trino.io/docs/current/admin/properties-query-management.html);
Flight — [Benchmarking Arrow Flight (arXiv 2204.03032)](https://arxiv.org/abs/2204.03032),
[C++/Java Flight perf #13980](https://github.com/apache/arrow/issues/13980).

## Engine vs gateway floor — the critical separation for goal-setting

Every through-Trino number carries **Trino's coordinator floor** (planning 20–50 ms + split sourcing +
scheduling ≈ 0.2–2 s wall-clock minimum), which applies identically to EVERY connector — it is NOT a
cqlite cost. So targets must split cleanly:

- **Server-direct Flight path** — bounded only by (a) cqlite SSTable read + row materialization and
  (b) the Arrow Flight transport floor (µs-class, negligible). This is where we can legitimately chase
  the tens-of-ms warm point-lookup and the columnar-native scan ceiling. Cold-parse (per #2385/#2412,
  the super-linear `SSTableReader::open`) is the real enemy here, not the wire.
- **Through-Trino path** — floored at ~0.2–2 s regardless of how fast cqlite is; here the honest goal
  is "don't add materially to Trino's floor" + scan throughput (rows/s/worker), never sub-second point
  latency. Pinot-class p99 (70–100 ms) is unreachable through Trino by construction.

Pinot/ClickHouse/Druid publish **engine-only** latencies (no external SQL gateway in the path) — their
70–100 ms p99 is not comparable to a through-Trino number; it IS comparable to our **server-direct**
Flight ceiling once cold-parse is eliminated.

## Implication for cqlite-flight numeric goals (honest, not aspirational)

- **Point lookup:** in-class floor = single-digit ms (OLTP), user-facing OLAP in-class = 70–100 ms p99.
  Server-direct realistic target: **tens of ms warm** (gated on #2385/#2412 cold-parse fix). Through
  Trino: accept the 0.2–2 s floor.
- **Interactive aggregation:** columnar-native systems set the bar (Pinot star-tree ms; ClickHouse
  2–10 GB/s/core). We are row-feed today (~29k rows/s/ring) — **1–2 orders below in-class**; closing it
  needs #2037/#941 (columnar), acknowledged in the landscape memo.
- **Full-scan:** in-class = 200M rows/s/core (ClickHouse), ~1 GiB/s/node (TiFlash), 600k rows/s/node
  (the Spark-Cassandra ancestor we most directly displace). Beating the ancestor's per-node rate while
  staying OLTP-isolated is the winnable near-term scan goal.
- **Concurrency:** in-class user-facing = thousands–100k+ qps (Pinot). We are at ~34 qps warm — a
  different weight class; the honest near-term concurrency goal is steady multi-pod scaling, not qps parity.
- **Transport:** never the bottleneck — Flight gives µs-latency / GB/s-throughput headroom well beyond
  what cqlite can currently feed it.
