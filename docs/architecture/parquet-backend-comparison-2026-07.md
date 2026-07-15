# Trino → cqlite-flight (live SSTables) vs Trino → Parquet backend — decision memo (2026-07)

**Status:** Research memo. Decision-grade comparison of the two architectures for serving
Trino analytics over Cassandra data.
**Audience:** CQLite maintainers + owner; anyone weighing cqlite-flight vs the standard lakehouse
(Hive/Iceberg-on-Parquet) pattern.
**Baseline:** round-11b field run, 2026-07-15, issue #2367 (see that thread for full context).
**Related in-tree:** `docs/architecture/cassandra-sidecar-parquet-projections.md` (the CDC/Parquet
position doc), `docs/architecture/issue-1045-spark-connector-research.md` (consistency semantics),
`cqlite-core/src/export/parquet.rs` (our own Parquet writer, feature `parquet`, epic #682),
`cqlite-cli/tests/duckdb_parquet_validation.rs` (in-tree DuckDB A/B seed).

## TL;DR verdict

The two architectures optimize for opposite ends of the query spectrum, and the choice is not
close on any single axis — it is a workload question.

- **cqlite-flight wins decisively on point reads, freshness, and operational simplicity of the
  data itself (zero-ETL).** The data *is* the live Cassandra data; there is no pipeline to run,
  no lag, and correctness (tombstones/TTL/LWW) is the read path's job, already solved.
- **Parquet-on-Trino wins decisively on full-scan aggregations, columnar projection, and
  `count(*)`** — the classic lakehouse strengths — *if* you accept an export pipeline and its
  freshness lag, and *if* you re-implement Cassandra's delete/TTL/LWW semantics in the export.

The honest one-liner: **cqlite-flight is an operational-data query gateway; Parquet-on-Trino is an
analytical warehouse.** They are not substitutes at the extremes; they overlap only in the middle
(bounded `LIMIT-k` scans), where both sit near Trino's ~1.3–2.3 s coordinator floor.

## Our measured baseline (MEASURED — round 11b, issue #2367)

Cluster: Trino 481 · 3 flight pods · Cassandra 5.0 RF=3 · ~1.94 M partitions/node · 2 SSTable gens ·
snapshot mode, connector 0.14.3, image `round11b`.

| Shape | Result |
|---|---|
| Point read (`WHERE pk=`) | 2.4–3.3 s wall; server-side cqlite cost ~ms (the ~2.3 s is JDBC/coordinator floor) |
| Warm `LIMIT 5` / `LIMIT 100` | ≈ 2.3 s (floor); server-side read cost ~2 ms cumulative over 9 calls |
| 8-thread warm throughput | ~34 qps, p50 227 ms, p99 366 ms, 0 errors |
| `count(*)` full ring | 66.2 s / 1,939,286 rows across 3 pods |
| Flight pod memory | 3–4 Mi idle, ~270–391 Mi peak under 8-thread load; 0 OOM/restart |

Key structural fact from the phase counters: server-side read cost is **~2 ms**; essentially all of
the ~2.3 s warm wall time is the Trino JDBC/coordinator round-trip, not CQLite.

## Published Trino-on-Parquet numbers (LITERATURE — cite with caveats)

Comparability is imperfect: published numbers vary with cluster size, JVM warmth, S3-vs-local
storage, file layout (sortedness, row-group size, bloom filters), and Parquet writer settings.
Treat these as order-of-magnitude, not head-to-head.

- **Trino has a structural coordinator floor of ~1.3–1.4 s** — it "cannot serve a sub-second
  user-facing dashboard regardless of dataset size" (e6data, 2026). This is the *same class* of floor
  we measure as our ~2.3 s JDBC floor; the delta is JDBC round-trip + our client harness, not engine
  work. Point-lookup response times of **1.03–1.41 s** are reported on tuned clusters (e6data).
- **Aggregate ceiling ~2.5 s without caching** for Trino on complex aggregates; StarRocks ~2 s in the
  same test (e6data / CelerData glossary, 2026) — i.e. Trino is *not* a low-latency serving engine even
  on columnar Parquet.
- **Parquet has no point index.** Pruning is row-group **min/max stats** + optional **bloom filters**
  only. For high-cardinality key columns (exactly the `WHERE pk=` case), min/max "can not help skip
  row groups… the entire column must be decoded to search for a particular value" (Apache DataFusion
  blog, 2025-03; Apache Parquet bloom-filter docs). Bloom filters help *equality* but must be written
  in, and Trino/Hive bloom support has historically been partial. **Net: a Parquet point read either
  scans matching row groups top-to-bottom or, without a bloom filter, scans the file.**
- **Parquet `count(*)` is near-free**: row counts live in the footer metadata; the engine reads no
  column data. This is Parquet's biggest structural advantage over our full-ring merge.
- A single small Parquet file *can* be queried with **millisecond** latency once metadata is cached
  (Apache Arrow blog, 2022-12) — but that is the file-read cost, still under Trino's coordinator floor
  at the user level.

## Structural analysis (REASONED — no measurement needed)

### Where Parquet-on-Trino structurally wins
1. **Full-scan aggregations & `count(*)`** — columnar, metadata-only counts, late materialization.
   Our `count(*)` is 66.2 s (full LWW merge of live SSTables); the Parquet equivalent is footer
   arithmetic — seconds or less. This is the single largest gap and it favors Parquet by ~10–100×.
2. **Column projection** — reading 2 of 40 columns touches 2 column chunks, not whole rows. Our SSTable
   read path materializes partitions.
3. **Compression ratio** — columnar encodings (RLE/dictionary/delta) + Snappy/Zstd typically beat
   row-oriented SSTable storage for analytical columns.
4. **Splittable, embarrassingly-parallel full scans** — row groups distribute cleanly across all Trino
   workers with no server round-trip and no per-partition merge.
5. **Mature, no custom server** — the Hive/Iceberg connectors are battle-tested; there is no bespoke
   Rust data plane to operate, patch, or reason about (the entire round-8/9/10/11 hardening saga on
   #2367 is work that a Parquet backend simply does not have).

### Where cqlite-flight structurally wins
1. **True point reads** — Cassandra's partition index (Summary → Index.db interval, #2412) resolves a
   key to a byte offset; the server-side cost is ~ms (measured). Parquet has *no* comparable index at
   1.9 M rows; its row-group pruning still decodes matching row groups. For `WHERE pk=`, cqlite-flight
   is doing an indexed seek while Parquet is doing a filtered scan.
2. **Zero-ETL freshness** — the data served *is* the live Cassandra data (staleness ≤ the
   snapshot-reuse window, default 3 s; LIVE mode = 0). A Parquet backend is only as fresh as its last
   export — minutes to hours (see ETL section).
3. **Snapshot-consistency semantics** — a Sidecar snapshot gives a consistent per-table read point;
   the connector reuses one snapshot per (ks, table) per window (#2356/#2306).
4. **Tombstone / TTL / LWW correctness for free** — the read path already reconciles deletes, expiries,
   and last-write-wins across SSTable generations (KWayMerger, query-semantics oracle). A Parquet export
   must *re-implement* all of this (see `cassandra-sidecar-parquet-projections.md`: a flushed SSTable is
   a *delta*, not a snapshot; a naive union resurrects deleted rows).
5. **Tiny idle footprint** — 3–4 Mi resident per pod idle; no standing warehouse, no object-store bill
   for a second copy of the data.

### Honest caveats on our side
- Our single-replica pinning is **CL.ONE semantics** (issue #1045 §2.4, owner-decided/documented): it
  may miss the newest write or resurrect data hidden by a tombstone on an un-read replica. Cassandra
  Analytics reads `blockFor(rf,dc)` replicas and quorum-merges. A Parquet export built from a proper
  quorum/repair'd source can be *more* consistent than our CL.ONE read on that axis.
- `count(*)` and large aggregations are our worst case by design.

## The ETL dimension (the strategic axis)

A Parquet backend is not free — it requires an **export pipeline**, and that pipeline is where the
lakehouse pattern pays its freshness and ops tax:

- **Our own exporter exists**: `cqlite-core/src/export/parquet.rs` (batch + streaming, Snappy row
  groups, epic #682), CLI `--out parquet`, Python `export_parquet()` / Node `exportParquet()`. Type
  mapping is analytics-grade (List/Map/Struct/Decimal128/Date32/Time64/UUID; only `duration`→Utf8
  remains, blocked on the `parquet` crate). Delta-scan export (#696/#705) carries `__writetime` /
  `__deleted` / `__ttl` so downstream can reconcile.
- **Freshness lag is inherent**: flush/compaction → detect (inotify on `TOC.txt`, or Sidecar poll, or
  commitlog CDC) → read → write Parquet → (ideally) commit to Iceberg/Delta. Realistic lag is
  **minutes to hours**, versus cqlite-flight's ≤ 3 s. For "how many orders in the last minute" this is
  disqualifying; for "revenue by region last quarter" it is irrelevant.
- **Semantic re-implementation is mandatory**: per `cassandra-sidecar-parquet-projections.md`, correct
  projections must carry write-timestamps and represent tombstones, or land in a table format
  (Iceberg/Delta) that does merge-on-read. Bare per-flush Parquet is silently wrong.
- **Operational cost (qualitative)**: a second full copy of the data (storage $), a projection service
  to run/monitor (schema caching, debounce, retries, compaction-vs-flush disambiguation), and a table
  format to commit into. cqlite-flight's ops cost is instead the 3 co-located Rust pods + Sidecar.
- **In-tree A/B seed**: `cqlite-cli/tests/duckdb_parquet_validation.rs` already exercises our Parquet
  output through DuckDB. That is the nucleus of a rigorous local A/B (below).

## Verdict table (shape-by-shape)

Legend: **M** = MEASURED (our round-11b) · **L** = LITERATURE (published Trino/Parquet) ·
**R** = REASONED (structural).

| Shape | cqlite-flight (live SSTables) | Trino → Parquet backend | Winner & rough margin |
|---|---|---|---|
| **Point read `WHERE pk=`** | 2.4–3.3 s wall, server ~2 ms (indexed seek) **[M]** | ~1.0–1.4 s tuned, but **no index** → row-group/bloom scan; degrades with cardinality **[L/R]** | **cqlite-flight** on server-side work & scaling; near-tie at the wall due to shared coordinator floor. Parquet has no partition index — our structural edge. |
| **`LIMIT-k` bounded scan** | ≈ 2.3 s floor, server ~2 ms **[M]** | ≈ Trino floor 1.3–2.5 s **[L]** | **~Tie** — both dominated by the coordinator floor. |
| **Full-scan aggregation / `count(*)`** | 66.2 s / 1.94 M rows (full LWW merge) **[M]** | footer-metadata count → sub-second to low-seconds **[L/R]** | **Parquet, ~10–100×.** Our worst case by design. |
| **Column projection / GROUP BY** | row/partition materialization **[R]** | columnar, reads only needed chunks **[L/R]** | **Parquet, large.** |
| **Freshness / staleness** | ≤ 3 s (reuse window); LIVE = 0 **[M]** | export lag: minutes–hours **[R]** | **cqlite-flight, decisive** (zero-ETL). |
| **Correctness (tombstone/TTL/LWW)** | solved in read path; oracle-gated **[M]** | must re-implement in export or lose it **[R]** | **cqlite-flight** (built-in) — Parquet needs Iceberg/Delta merge-on-read. |
| **Consistency (replica set)** | CL.ONE (single-replica pin), documented **[M]** | as strong as the export source (can be quorum/repaired) **[R]** | **Parquet** can be stronger here; our known caveat (#1045). |
| **Ops cost** | 3 co-located Rust pods + Sidecar; no data copy **[M/R]** | export service + 2nd data copy + table format; no custom read server **[R]** | **Split**: cqlite simpler on *data* (zero-ETL); Parquet simpler on *read engine* (mature, no bespoke server). |
| **Memory footprint** | 3–4 Mi idle, ~391 Mi peak/pod **[M]** | Trino worker heaps (GBs) + object store **[R]** | **cqlite-flight** on the serving tier. |

## Proposed follow-up experiment (round-12/13 candidate for the easy-db-lab kit)

A rigorous **same-hardware A/B** would convert the REASONED/LITERATURE rows above into MEASURED ones.
The one table (~1.94 M partitions) already under test on the round-11b cluster is the fixture.

**Setup (same cluster, same Trino 481):**
1. Export the same table to Parquet via our own exporter: `cqlite --out parquet` (or the streaming
   writer) over all live SSTables for a *current-snapshot* projection (point
   `open_with_discovered_sstables()` at every generation so the LWW merge runs), carrying
   `__writetime`/`__deleted` so the Parquet is semantically honest.
2. Land the Parquet in the object store the lab cluster already uses; register it as a Hive **and** an
   Iceberg table (two sub-arms — Iceberg adds bloom filters + better stats + merge-on-read).
3. Configure the Trino Hive/Iceberg connector against it. Write the Parquet **sorted by partition key**
   with **bloom filters on the key** in one arm (best case for point reads) and unsorted in another
   (worst case) to bound the point-read spread.

**Query set (identical to round-11b):** point read `WHERE pk=`; `LIMIT 5`/`LIMIT 100`; `count(*)`;
one `GROUP BY`/aggregation; the 8-thread warm throughput loop.

**Measure:** wall latency, rows/s, qps, p50/p99, Trino worker CPU/heap, bytes scanned per query
(Parquet's key tell — how much of the file a point read actually reads), and export wall time + output
size (the ETL cost line).

**What it settles:**
- The real point-read gap (indexed seek vs bloom/row-group scan) at 1.9 M rows, sorted vs unsorted.
- The real `count(*)`/aggregation win magnitude for Parquet on *our* data and hardware.
- The Parquet compression ratio vs on-disk SSTable size for this schema.
- The end-to-end export cost (time + storage) that the verdict table currently only reasons about.
- Whether Iceberg's stats/bloom close enough of the point-read gap to matter.

**Explicitly out of scope for the A/B:** freshness (definitionally won by cqlite-flight — the Parquet
is a point-in-time copy) and multi-replica consistency (orthogonal to storage format).

## Bottom line

Pick by workload, not by preference. **Operational/point/fresh → cqlite-flight.**
**Warehouse/scan/aggregate/historical → Parquet-on-Trino.** The interesting product position is that
CQLite already ships *both halves*: the live-read Flight path **and** the Parquet exporter (#682) that
feeds the lakehouse — so the honest recommendation is not "either/or" but "cqlite-flight for the
serving tier, our own Parquet export for the analytical tier," with the A/B above to quantify exactly
where the crossover sits.

## Sources (literature legs)

- e6data, *Trino Query Performance Optimization* (2026) — coordinator floor ~1.3 s; point-lookup
  1.03–1.41 s; aggregate ceiling ~2.5 s. <https://www.e6data.com/query-and-cost-optimization-hub/how-to-optimize-trino-query-performance>
- CelerData, *Trino Query Optimization* glossary (2026) — Trino vs StarRocks aggregate latency.
  <https://celerdata.com/glossary/trino-query-optimization>
- Apache DataFusion blog, *Parquet Pruning* (2025-03-20) — min/max useless for high-cardinality keys;
  full column decode. <https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/>
- Apache Parquet, *Bloom Filter* file-format docs — equality-only, must be written in.
  <https://parquet.apache.org/docs/file-format/bloomfilter/>
- Apache Arrow blog, *Querying Parquet with Millisecond Latency* (2022-12-26) — metadata-cached small
  file reads. <https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/>
- StarRocks, *TPC-DS Benchmark* / Trino-vs-StarRocks — Trino Hive-external latency context.
  <https://docs.starrocks.io/docs/benchmarking/TPC_DS_Benchmark/>
