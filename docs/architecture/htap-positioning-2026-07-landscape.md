# The HTAP / hybrid-analytics landscape — where Trino → cqlite-flight sits (2026-07)

**Status:** Research memo. Landscape survey positioning cqlite-flight (Trino analytics directly on
live Cassandra SSTables, zero-ETL) against the broader HTAP / hybrid-analytics field.
**Audience:** CQLite maintainers + owner; anyone asking "isn't this just HTAP / just Spark-on-Cassandra?"
**Baseline:** round-11b field run, 2026-07-15, issue #2367.
**Related in-tree:** `docs/architecture/parquet-backend-comparison-2026-07.md` (the cqlite-flight vs
Parquet decision memo), `docs/architecture/cassandra-sidecar-parquet-projections.md` (CDC/Parquet
position), `docs/architecture/issue-1045-spark-connector-research.md` (Spark-connector consistency
semantics). Epics referenced: #2037 (ArrowMemtable OLAP), #941 (DataFusion provider).

## TL;DR

The market has four ways to run analytics near an operational store. cqlite-flight belongs to the
rarest one — **scan the OLTP storage format directly, out-of-process** — and is, as far as this
survey found, the only member of that family for Apache Cassandra that is OLTP-isolated and
seconds-fresh with zero pipeline. Our current OLAP *class* is "row-feed into Trino" (no columnar
replica), which is where the honest gaps vs TiFlash-class systems live.

## Taxonomy — one row per family

| Family | Representative systems | Architecture | Typical freshness | OLAP perf class | OLTP isolation (does analytics load the serving path?) | Ops burden |
|---|---|---|---|---|---|---|
| **1. Dual-format replica HTAP** | TiDB + TiFlash; SingleStore; MySQL HeatWave; AlloyDB columnar engine; Snowflake Unistore / Hybrid Tables | Second, columnar copy of the data kept in sync inside (or beside) the OLTP engine — Raft-learner columnar replica (TiFlash), unified rowstore+columnstore LSM (SingleStore), in-memory column store (HeatWave), WAL-derived columnar store (AlloyDB), purpose-built hybrid engine (Unistore) | **ms–sub-second.** TiFlash Raft-learner replication; HeatWave merges to column store every ~200 ms / 64 MB buffer | **Columnar-native.** Vectorized scans, MPP aggregation, pushdown — the strong end of the spectrum | **Strong.** Analytics hits the *columnar replica*, physically isolated from the rowstore serving path (TiFlash's explicit design goal) | **High-ish but managed.** You run/pay for a second columnar replica set (extra nodes/RAM); mostly automatic once provisioned |
| **2. Postgres-attached OLAP** | pg_duckdb; Hydra; ParadeDB (pg_analytics, now folded into pg_search); read-replica + DuckDB patterns | Embed a columnar/vectorized executor (DuckDB) *in-process* over the heap or over external Parquet/Iceberg on object store | **Live** over the heap; **export-lagged** (min–hours) when querying Parquet/lake tables | **Columnar-native executor**, but over row-store heap it re-reads row pages (feed penalty) unless data is in Parquet | **Weak–medium.** In-process DuckDB shares the Postgres box's CPU/RAM/buffer cache with OLTP; heavy scans contend. Read-replica variants isolate | **Low–medium.** One extension, no new cluster; but no columnar copy of hot data unless you add Parquet/ETL |
| **3. CDC → OLAP store** (the common Cassandra answer today) | Cassandra CDC/Debezium → ClickHouse / Apache Pinot / Apache Druid / StarRocks; generic Debezium → lakehouse | Stream change events (commitlog/CDC → Kafka) into a separate purpose-built columnar OLAP database | **seconds–minutes** (streaming) to **hours** (batch), plus pipeline lag/backpressure | **Columnar-native, best-in-class** for user-facing low-latency analytics (Pinot single-digit-ms p99) | **Strong** at query time (fully separate cluster) — but CDC *capture* adds write-path/commitlog overhead on the OLTP side | **Highest.** A whole second database + Kafka + Debezium + schema mapping + a pipeline to run, monitor, and reconcile (delete/TTL/LWW semantics re-implemented downstream) |
| **4. Scan-the-OLTP-storage direct** (OUR family) | **cqlite-flight** (this project); DSE Analytics / Spark-Cassandra-connector (the ancestor); Rockset converged index (historical, RocksDB-on-S3); external RocksDB-SST / MyRocks readers | Read the OLTP engine's own on-disk files (SSTables / SSTs) and serve analytics from them — no second format, no pipeline | **Snapshot-fresh:** cqlite-flight reads Sidecar hardlink snapshots → **seconds** (bounded by snapshot cadence). Spark-connector: **live** but via the serving path | **Row-feed** into the analytics engine (Trino / Spark). Not columnar-native today — this is the family's shared weakness | **Split.** cqlite-flight: **near-zero** — reads immutable SSTable snapshots via Sidecar hardlinks, off-JVM, serving path untouched. **Spark-connector: poor** — CassandraRDD hits every node's read path in the live JVM (ALLOW FILTERING, heap pressure) | **Low.** cqlite-flight: no extra cluster/replica/pipeline — just the Sidecar + a stateless Flight/Trino connector. Spark: a Spark cluster, but no second copy |

### Notes / sources per family

- **TiFlash** is a Raft-*learner* columnar replica (DeltaTree engine, Parquet-like LZ4 blocks);
  analytics read the replica under snapshot isolation, strongly isolated from TiKV rowstore
  ([PingCAP VLDB 2020](https://www.pingcap.com/blog/vldb-2020-tidb-a-raft-based-htap-database/),
  [TiFlash overview](https://docs.pingcap.com/tidb/stable/tiflash-overview/)).
- **SingleStore** = one unified rowstore+columnstore LSM layout, no copy
  ([SingleStore blog](https://www.singlestore.com/blog/pushing-htap-databases-forward-with-singlestoredb/)).
  **HeatWave** = in-memory column store, ~200 ms / 64 MB merge cadence
  ([HTAP survey, arXiv 2404.15670](https://arxiv.org/html/2404.15670v1)).
- **AlloyDB** analyzes the WAL and autonomously maintains an in-memory+on-disk columnar
  representation; planner routes point-reads to row store, aggregates to columnar — no ETL, no
  schema change ([Google Cloud blog](https://cloud.google.com/blog/products/databases/alloydb-for-postgresql-columnar-engine)).
- **Snowflake Unistore / Hybrid Tables** GA at BUILD 2024 — one engine, double-digit-ms point ops
  beside analytical queries ([Snowflake GA blog](https://www.snowflake.com/en/blog/unistore-general-availability/)).
- **pg_duckdb** embeds DuckDB *in the Postgres process*; reads heap or Parquet/Iceberg/Delta on
  S3/GCS ([MotherDuck](https://motherduck.com/blog/pg_duckdb-postgresql-extension-for-duckdb-motherduck/),
  [duckdb/pg_duckdb](https://github.com/duckdb/pg_duckdb)). ParadeDB's pg_analytics is archived,
  folded into pg_search ([paradedb/pg_analytics](https://github.com/paradedb/pg_analytics)).
- **CDC→OLAP**: Debezium→Kafka→ClickHouse/Pinot/Druid/StarRocks is the established pattern; Pinot
  runs user-facing analytics at hundreds of billions of rows with single-digit-ms p99
  ([StarTree](https://startree.ai/resources/a-tale-of-three-real-time-olap-databases/),
  [Tinybird](https://www.tinybird.co/blog/fastest-database-for-analytics)).
- **Spark-Cassandra-connector** partitions by token range and reads through the live serving path;
  docs explicitly note ALLOW-FILTERING full scans and executor-heap pressure are acceptable only
  because "queries won't execute very often"
  ([connector FAQ](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/FAQ.md),
  [Databricks](https://www.databricks.com/blog/2015/06/16/zen-and-the-art-of-spark-maintenance-with-cassandra.html)).
- **Rockset** built a converged (row+column+search) index over RocksDB, replicating SST files to S3
  — the closest "serve analytics from the OLTP engine's own SST files" precedent, though as a
  separate managed cloud store, not in-place
  ([Rockset/Medium](https://medium.com/rocksetcloud/how-rocksets-converged-index-powers-real-time-analytics-c6c2e6066d9e)).

## Where cqlite-flight is differentiated

Within the Cassandra ecosystem, the *only* prior general-purpose analytics answers are Family 3
(CDC → a second OLAP database) and Family 4's ancestor (Spark-on-Cassandra). cqlite-flight is
differentiated on three axes simultaneously — and the combination is what is close to unique:

1. **Zero-ETL / zero second copy.** The data queried *is* the live Cassandra data on disk. No
   Kafka, no Debezium, no ClickHouse/Pinot cluster, no columnar replica to provision. Correctness
   (tombstones / TTL / LWW reconciliation) is the read path's job and is already solved — CDC
   pipelines must re-implement all of it downstream and reconcile drift. Contrast Families 1 and 3,
   which both maintain a second physical representation.

2. **OLTP-isolated by construction.** cqlite-flight reads **immutable SSTable snapshots via
   Cassandra Sidecar hardlinks**, out-of-process from the Cassandra JVM. A full-scan analytics job
   does not touch the serving read path, the heap, or the page cache the way CassandraRDD /
   Spark-on-Cassandra does (that ancestor hammers the same JVM with ALLOW-FILTERING scans). This is
   the single biggest practical improvement over the Cassandra status quo: **analytics that can't
   knock over the operational cluster.**

3. **Seconds-fresh with no pipeline.** Freshness is bounded by snapshot cadence (seconds), not by
   an ETL/CDC lag measured in minutes-to-hours. We land between the ms-fresh dual-format HTAP
   systems and the minutes-fresh CDC pipelines — but without owning either a columnar replica or a
   pipeline.

**Positioning one-liner:** cqlite-flight is the *zero-ETL, OLTP-isolated, seconds-fresh* analytics
gateway for Cassandra — Family 4 done with the isolation of Family 1 and the ops footprint of an
extension, not a cluster.

**Current OLAP class (honest):** *row-feed into Trino*, not columnar-native. Measured round-11b:
warm ~34 qps interactive point/LIMIT reads; ~29k rows/s/ring on full-ring scan. That is Trino
pulling rows over Arrow Flight, not vectorized columnar aggregation. Columnar is on the roadmap
(#2037 ArrowMemtable OLAP, #941 DataFusion provider), not shipped.

## Three honest gaps vs the field

1. **No columnar replica → aggregation ceiling.** Families 1 and 3 keep a real columnar copy;
   they win decisively on `GROUP BY`/`count(*)`/scan-heavy aggregation and hit single-digit-ms p99
   at scale (Pinot, TiFlash). We row-feed Trino, so large aggregations are bounded by row
   materialization + the coordinator floor, not by vectorized columnar execution. Closing this is
   the point of #2037 / #941; until then, heavy OLAP is not our lane.

2. **Freshness is snapshot-cadence, not truly live.** Dual-format HTAP is ms-fresh and
   Spark-on-Cassandra is live-through-the-serving-path. Our Sidecar-hardlink snapshot model is
   seconds-fresh *and* isolated — a deliberate trade — but it is not "read the write you just made"
   the way an in-engine columnar replica is. Mutations still in the memtable / not yet flushed +
   snapshotted are not visible until the next snapshot.

3. **We serve one engine's storage, not a general HTAP engine.** TiDB/SingleStore/AlloyDB *are*
   the database — one system, one optimizer, transactional guarantees across both workloads.
   cqlite-flight is a read-only analytics sidecar bolted onto Cassandra: no writes, no
   cross-workload transactions, no query optimizer beyond Trino's. We deliberately trade "unified
   system" for "no second system to run" — but buyers comparing against a TiFlash-class product
   should understand it is not an apples-to-apples HTAP engine.
