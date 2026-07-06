# ArrowMemtable — a coordinator-native OLAP path for Cassandra (exploration research record)

**Epic:** [#2037](https://github.com/pmcfadin/cqlite/issues/2037) · **Status:** exploration (Backlog by design) · **Date:** 2026-07-05
**Provenance:** owner-directed exploration session (2026-07-05) + fan-out research workflow (Haiku/Sonnet gatherers; per-system precedent survey, Cassandra-ecosystem prior art, Java-side requirements extraction from the #1807 corpus; three structured-output gatherer failures — GreptimeDB, Tonbo, HoraeDB — re-run as plain-markdown backfill agents; §11 added 2026-07-05 by a second fan-out workflow: 10 format profiles + research bibliography/HTAP precedents + outside-the-box directions, 13/13 gatherers; §12 added same day by a third workflow: 2 constants gatherers + 7 scenario simulators + adversarial arithmetic verifier + consolidator, 12/12, with one flow-lead editorial correction to the freshness axis, noted in §12.4).
**Lineage:** builds on the verified [#1807 CEP-11 memtable plugin design](../storage%20engine/memtable-plugin-design.md) (extend-TrieMemtable, on-demand `nb` tail export) and the [#1934 engine thesis](https://github.com/pmcfadin/cqlite/issues/1934). Owner standing decision: **no CDC** (2026-07-03).

---

## 1. Thesis

Present an **OLAP path on the Cassandra coordinator itself** — not a separate system (Spark cluster, external ETL, sidecar-driven bulk export). You cannot do that cleanly "from query to memory to disk" without thinking the entire path as one design:

- **ArrowMemtable** — a CEP-11 memtable plugin, `extends TrieMemtable` (forced by two `instanceof` gates; §5). The in-memory structure stays a mutable row-oriented trie — *the memtable's analytics read-out form is Arrow, not its storage form*. This is exactly how every Arrow-native system uses the word (§3): mutable buffer inside, immutable Arrow out.
- **Node-local CQLite scan engine** — reads the node's flushed SSTable set **plus the memtable tail as one unit per table**, runs CQLite's existing, parity-tested **k-way LWW merge** (cell-grain timestamps, TTL, all four tombstone grains), and produces **Arrow batches post-merge**. Cassandra semantics are fully resolved *before* data ever becomes columnar, so the OLAP output needs no semantics envelope at all.
- **Coordinator OLAP verb** — fans token-range-scoped scans out to replicas and returns Arrow. The fan-out logic exists once already in the CQLite Trino connector (+ #1336's RF-correct per-range replica scoping); this moves it in-path.

### The dissolving insight

Community proposals in the shape of **"Parquet-attached SSTables"** (by analogy to SAI) fail on two counts:

1. **A per-SSTable columnar file cannot be scanned independently.** Correctness lives *across* SSTables: a tombstone in generation 47 shadows rows in generation 12. SAI survives its per-SSTable lifecycle only because index results are *consulted by* Cassandra's reconciling read path — a Parquet attachment is meant to be *scanned instead of* that path. Either the attachment carries a full Cassandra-semantics envelope **and** a tombstone-aware columnar merger exists at query time, or the results are wrong.
2. **A materialized second format is rewritten by every compaction forever** — dual-format compaction, doubled write amplification, permanent lifecycle coupling.

The tombstone-aware merger that approach is missing **already exists: it is CQLite.** Which dissolves the need for the second format entirely: the columnar copy never has to exist as a correctness artifact. Per-generation Arrow segments exist only as a **cache** (§6) — invalidated exactly at generation death, rebuilt lazily, cold-cache = slower-never-wrong. Compaction keeps one job.

### Architecture

```
                    Cassandra node (one path, one unit per table)
┌──────────────────────────────────────────────────────────────────────┐
│  CQL writes → ArrowMemtable (extends TrieMemtable) → nb SSTables     │
│                     │ tail handoff (§4)                  │ unchanged │
│                     ▼                                    ▼           │
│              ┌─ CQLite node-local scan engine ────────────┐          │
│              │  CompositeSource(data_dir + tail)          │          │
│              │  → k-way LWW merge (tombstone-correct)     │          │
│              │  → Arrow batches out (post-merge, clean)   │          │
│              │  [per-generation Arrow cache, §6]          │          │
│              └────────────────┬───────────────────────────┘          │
│  coordinator OLAP verb ───────┘   (fan-out to peers by token range)  │
└──────────────────────────────────────────────────────────────────────┘
```

**Row format until the data stops mutating; Arrow from the first immutable moment; never decode `nb` twice.**

---

## 2. The serde argument, stated precisely

Serde cost = (number of format conversions) × (times each is paid). "Arrow everywhere" attacks the first lever; the second matters more here:

- Today's bill: `nb parse → Value → Arrow` paid **per scan, per query, forever** over the flushed ~99% of bytes. Our own audits measured the shape of this (the 88-byte `Value` materialization in every decode — read-path audit epics A–G; materialize-then-emit — export audit AA–AF).
- Done right, each byte is converted **once per generation** into a format every downstream consumer (Flight, DataFusion #941, Trino, pyarrow) uses with zero further serde. The win is moving the one unavoidable conversion from query-time to generation-birth and amortizing it over every future query.

Where Arrow **cannot** go, and why that is fine:

1. **The mutation path.** The native protocol is row-grain single mutations; the memtable does cell-grain upsert reconciliation. Columnarizing ingest buys nothing (no scans there) and costs latency. Every precedent system agrees (§3).
2. **`nb` is load-bearing.** Cassandra reads its own files for OLTP reads, compaction, repair, streaming. Arrow inside Cassandra can only ever be a *second copy* — hence cache-not-copy, engine-side.
3. **Point reads.** Columnar layouts pessimize partition lookup. Arrow is a scan format.

Honest footnotes: **Parquet is not Arrow** (compressed/encoded; cold reads pay a vectorized decode; true zero-serde storage = Arrow IPC, 3–10× bigger — IOx resolves this hot-Arrow/cold-Parquet). **Flight is zero-serde, not zero-copy over a network** — no transcoding, but bytes still move.

---

## 3. Precedent survey: columnar/Arrow memtables in other LSM databases

### 1. Comparison Table

| System | LSM? | In-memory structure | Columnar boundary | Delete semantics grain | Analytics freshness |
|---|---|---|---|---|---|
| **InfluxDB 3.0 / IOx** | LSM-like, not classic (time-bucketed Parquet "generations," no global sort/level structure) | Row-form WAL op-log (~1s) → immediately materialized as Arrow `RecordBatch`es in `QueryableBuffer` | WAL flush (~1s) makes it columnar in-memory; `sort_dedupe_persist()` seals immutable gen1 Parquet at 10 min / 600-WAL-file cadence | Whole table/database only (shipped product); row-level delete exists solely as an undocumented Enterprise 3.10 beta, discouraged in production | Live, read-your-writes — queries fan out to unflushed buffer/ingester and union with persisted Parquet; no fixed staleness window |
| **Apache Paimon** | Classic LSM (sorted runs, L0→Ln compaction) | Row-oriented, PK-sorted write buffer (per bucket) | At buffer flush — L0 files are already Parquet (or configurable per-level format) | Whole-row, keyed on PK (Deduplicate engine); Partial Update refuses deletes by default and never applies incoming NULL as an overwrite — no true cell tombstone | Checkpoint/commit-bound, typically 1–10 min; zero read visibility into the writer's in-memory buffer, batch or streaming |
| **Apache Kudu** | LSM-like hybrid (row MemRowSet + columnar DiskRowSet + per-rowset deltas) | Row-oriented, in-memory concurrent B-tree (MemRowSet) | At flush — DiskRowSet's CFile base data is columnar (RLE/dictionary/bit-packed); mutations land in separate delta B-trees, never rewrite base columns | (rowid, timestamp) tuple — REDO deltas merged into UNDO on major compaction | Live — READ_LATEST (default) sees unflushed MemRowSet immediately; READ_AT_SNAPSHOT trades immediacy for a safe-timestamp wait |
| **ClickHouse MergeTree family** | LSM-like but explicitly not classic LSM — no memtable/WAL, flat part hierarchy | None — sorted briefly then written directly to disk as immutable columnar parts | Immediate at insert; no row-oriented staging tier exists at all | Row-grain (ReplacingMergeTree version, CollapsingMergeTree sign-pair, lightweight `_row_exists` mask) or whole-part (mutations) | Eventual/stale until background merge; `FINAL` forces point-in-time correctness at a documented 10x+ latency cost |
| **Apache Doris / StarRocks** | Classic LSM-tree variant | Row-oriented, PK-sorted MemTable (Doris default 200MB) | At flush — Rowset wraps 1+ immutable columnar Segment files (64KB pages/column) | Row-grain delete vector: Doris Delete Bitmap keyed (rowset_id, segment_id, version); StarRocks DelVector per segment per rowset | Write-time dedup visible after transaction commit; unflushed MemTable rows are **not** visible to concurrent queries |
| **GreptimeDB (Mito engine)** | Yes — LSM-tree, region-based (per-table shards) | Default `time_series` memtable: columnar per-series buffers keyed by primary key (`BTreeMap<PK, Arc<RwLock<Series>>>`), each holding timestamp/sequence/op_type/field column vectors; a newer/experimental `partition_tree` (merge-tree) variant is write-optimized but its current status is in flux in-source | Memtable is already columnar internally; an internal `Batch` type is adapted to Arrow `RecordBatch` via `BatchToRecordBatchAdapter`; flush writes immutable, PK+time-sorted Apache Parquet SSTs | Row-level only, via a per-row `__op_type` (Put/Delete) column plus `__sequence`, reconciled during merge-scan/dedup by `(primary_key, timestamp)` with sequence as tiebreaker | Live/read-your-writes — every scan merges unflushed memtables with on-disk SSTs by sequence; no snapshot/staleness boundary found |
| **Tonbo** | LSM ("merge-tree optimized for object storage"): WAL → MemTable → Parquet SSTables | Arrow-native even pre-flush: `crossbeam-skiplist::SkipMap<KeyTsOwned, DynMutation<BatchRowLoc, DeleteRowLoc>>` MVCC index over pre-allocated `RecordBatch` slots (no row structs — writes land as Arrow arrays from the start) | None — writes are ingested as `RecordBatch`es directly; "SSTable" flush is literally immutable Parquet | Row/primary-key grain only — a delete is a `DynMutation::Delete` pointing into a keys-only "delete sidecar" `RecordBatch` (+ commit_ts), reconciled at merge via an `_tombstone` marker column; no range/cell/partition tombstones | Yes — MVCC read-your-writes; unflushed memtable rows are visible to scans (example `02_transaction`), with `02b_snapshot` giving consistent point-in-time reads concurrent with writes |
| **Apache HoraeDB** | LSM-tree variant tuned for time-series (per-table WAL + memtable + Parquet SST + background compaction); podling **retired 2026-03-03** without graduating | Legacy analytic-engine: per-table skiplist memtable (agatedb-derived, Arena-bounded); an unshipped 2024/2025 rewrite (`columnar_storage`/`metric_engine`) drops the memtable entirely | Parquet SSTs segmented by `segment_duration`; Arrow surfaces in DataFusion query execution (RecordBatch streams) and, in the rewrite, directly in the write API (`WriteRequest{batch: RecordBatch}`) | No row-level DELETE/tombstone found; deletion is SST/file-level only — compaction purges expired/duplicate SSTs by segment min/max timestamp (coarse TTL, not per-row) | Yes — `Table::read()` sources data from **both** SST and Memtable together, so unflushed writes are query-visible pre-flush |
| **RocksDB / ScyllaDB / HBase / SlateDB** (OLTP contrast group) | Classic LSM-trees (LevelDB- and Cassandra/BigTable-derived) | Row-oriented skip-list / red-black tree memtables (all four) | **None** — SSTables stay row-oriented on disk; columnar analytics requires a separate post-flush ETL step outside the core storage path | Point/range tombstone (RocksDB `Delete`/`DeleteRange`), cell/row/partition tombstone with `gc_grace_seconds` (Scylla/HBase/Cassandra lineage) | MVCC snapshot isolation, but unflushed memtable data is invisible to distributed/analytics reads until flush + compaction |

### 2. Per-System Detail

#### InfluxDB 3.0 / IOx
IOx keeps the LSM-family shape — buffer, seal immutable segments, background-merge — but its "segments" are time-bucketed Parquet files with no global sort order across gen1 files, and compaction generations are duration multiples rather than size tiers or LSM levels. Verified from source (`queryable_buffer.rs`), the write path batches ops for ~1 second into a WAL op-log, then immediately materializes them as Arrow `RecordBatch`es inside the `QueryableBuffer` — columnar conversion happens well before any durability-to-object-store step. Every `--gen1-duration` (default 10 min) or 600-WAL-file threshold, `sort_dedupe_persist()` runs a DataFusion sort+dedupe pass and writes immutable, explicitly "un-compacted and not indexed" gen1 Parquet files; a separate background Compactor (Enterprise/Clustered only — Core has none) later merges these into gen2/gen3. Delete support is the standout weakness: the shipped product supports only whole-table/whole-database delete, and row-level delete exists solely as an undocumented, production-discouraged Enterprise 3.10 beta feature. Freshness is the standout strength: both architecture generations are read-your-writes by design — queries union live buffer/ingester data with persisted Parquet chunks with no fixed staleness window, at the cost of either a per-query RPC fan-out (Clustered) or an unbounded query-time dedup cost on gen1 overlap (Core, which ships with no compactor at all). Sources: [InfluxDB 3.0 architecture](https://www.influxdata.com/blog/influxdb-3-0-system-architecture/), [Designing a Parquet Catalog](https://www.influxdata.com/blog/designing-a-parquet-catalog-for-influxdb-iox/), [Core/Enterprise architecture](https://www.influxdata.com/blog/influxdb3-core-enterprise-architecture/), [queryable_buffer.rs](https://raw.githubusercontent.com/influxdata/influxdb/main/influxdb3_write/src/write_buffer/queryable_buffer.rs), [WAL deepwiki](https://deepwiki.com/influxdata/influxdb/3.2-write-ahead-log-(wal)), [delete CLI](https://docs.influxdata.com/influxdb3/enterprise/reference/cli/influxdb3/delete/), [Enterprise release notes](https://docs.influxdata.com/influxdb3/enterprise/release-notes/), [config options](https://docs.influxdata.com/influxdb3/enterprise/reference/config-options/), [Clustered storage engine](https://docs.influxdata.com/influxdb3/clustered/reference/internals/storage-engine/).

#### Apache Paimon
Paimon is a textbook LSM applied to a lake substrate: writes buffer per-bucket in a PK-sorted in-memory buffer, flush as immutable sorted-run files (Parquet by default, mixable per level), and are periodically compacted L0→Ln — with each bucket owning an independent LSM tree and no global cross-bucket sort order. The columnar boundary sits right at that first flush: there is no persistent row-oriented staging tier by default, only an optional Avro changelog file for CDC-style consumption. Delete fidelity is genuinely weak versus cell-grain LWW: the default Deduplicate engine's DELETE removes the whole row keyed on PK, and Partial Update — the one engine with field-level semantics — refuses deletes unless explicitly configured, and even then offers only whole-row-on-delete per declared sequence-group; critically, incoming NULLs are *never* applied as overwrites, so a partial-update write cannot express "set this cell to NULL" at all. Freshness is hard-bound to the Flink checkpoint/commit cadence (typically 1–10 min, production guidance ~1–5 min): a snapshot only becomes visible — to batch or streaming readers alike — once a full 2PC commit completes, with zero read path into the writer's live buffer. Deletion-vector (MOW) mode adds a further documented latency tax: L0 files are only visible after compaction to L1. Sources: [merge-engine](https://paimon.apache.org/docs/master/primary-key-table/merge-engine/), [partial-update](https://paimon.apache.org/docs/master/primary-key-table/merge-engine/partial-update/), [table-mode](https://paimon.apache.org/docs/1.0/primary-key-table/table-mode/), [compaction](https://paimon.apache.org/docs/master/primary-key-table/compaction/), [changelog-producer](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/), [sequence-rowkind](https://paimon.apache.org/docs/master/primary-key-table/sequence-rowkind/), [deletion-vectors](https://paimon.apache.org/docs/0.8/primary-key-table/deletion-vectors/), [PIP-16](https://cwiki.apache.org/confluence/display/PAIMON/PIP-16%3A+Introduce+deletion+vectors+for+primary+key+table), [fileformat spec](https://paimon.apache.org/docs/master/concepts/spec/fileformat/), [basic-concepts](https://paimon.apache.org/docs/master/concepts/basic-concepts/), [write-performance](https://paimon.apache.org/docs/master/maintenance/write-performance/), [Vanlightly consistency model](https://jack-vanlightly.com/analyses/2024/7/3/understanding-apache-paimon-consistency-model-part-1), [Alibaba Cloud latency/consistency](http://www.alibabacloud.com/help/en/flink/use-cases/timeliness-and-consistency-of-paimon).

#### Apache Kudu
Kudu is the closest existing precedent to the proposed CQLite shape: a row-oriented in-memory MemRowSet (concurrent B-tree, MVCC-linked) flushes to an immutable columnar DiskRowSet (CFile, per-column RLE/dictionary/bit-packed encoding), with mutations to already-flushed rows tracked separately in a per-rowset DeltaMemStore/DeltaFiles keyed by (rowid, timestamp) rather than rewriting encoded columns in place. Deletes are REDO records that major delta compaction later folds into base data and converts to UNDO records for historical (snapshot) visibility — a per-rowid, per-timestamp grain, not a cell-level tombstone and not a cross-rowset propagation problem. Freshness is live by default: READ_LATEST scans see unflushed MemRowSet data immediately, while READ_AT_SNAPSHOT trades that immediacy for waiting until a target timestamp is "safe." The dual-format design here is exactly the mechanism the survey is testing against: because encoded columnar data cannot be mutated in place, every update forces delta accumulation in a separate structure, and reads of frequently-updated rows must sequentially scan all accumulated deltas with no predicate pushdown to skip them — a documented, verified overhead. Sources: [Kudu paper](https://kudu.apache.org/kudu.pdf), [tablet design doc](https://github.com/apache/kudu/blob/master/docs/design-docs/tablet.md), [Cloudera read/write paths](https://www.cloudera.com/blog/technical/apache-kudu-read-write-paths.html), [doanduyhai internals](https://www.doanduyhai.com/blog/?p=13466), [background tasks](https://kudu.apache.org/docs/background_tasks.html), [transaction semantics](https://kudu.apache.org/docs/transaction_semantics.html), [FAQ](https://kudu.apache.org/faq.html), [known issues](https://kudu.apache.org/docs/known_issues.html).

#### ClickHouse MergeTree family
ClickHouse is the outlier in the survey: it has no memtable and no WAL at all — inserts are sorted briefly and written directly to disk as immutable columnar parts, with a flat, level-free part hierarchy rather than classic LSM tiering. Columnar layout is the *only* on-disk representation; the boundary is immediate at insert, not a later flush stage. Delete/update semantics are scattered by engine: ReplacingMergeTree inserts new PK-keyed versions and relies on background merge (or `OPTIMIZE ... FINAL`) to retain the latest; CollapsingMergeTree pairs +1/-1 sign rows and can fail to fully collapse if merges lag; lightweight deletes use a cheaper `_row_exists` mask; full mutations rewrite entire affected parts on disk. Freshness is explicitly eventual — duplicates and cancellations coexist with current versions until background merges complete, and the only way to force point-in-time correctness is the `FINAL` modifier, at a documented 10x+ query latency penalty. This is the sharpest illustration in the survey of the tension between skipping a mutable staging tier (fast writes, immediate columnar) and paying for merge-time correctness at read time instead. Sources: [architecture](https://clickhouse.com/docs/development/architecture), [ReplacingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree), [CollapsingMergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/collapsingmergetree), [purpose-built engines](https://clickhouse.com/blog/updates-in-clickhouse-1-purpose-built-engines), [MergeTree internals](https://medium.com/@anshs5103/clickhouse-under-the-hood-mergetree-lsm-architecture-and-disk-level-optimizations-13ea85fc684d), [Altinity ReplacingMergeTree](https://altinity.com/blog/clickhouse-replacingmergetree-explained-the-good-the-bad-and-the-ugly), [8 hours learning ClickHouse](https://vutr.substack.com/p/i-spent-8-hours-learning-the-clickhouse), [CollapsingMergeTree deletes](https://chistadata.com/deletes-and-updates-in-clickhouse-using-collapsingmergetree/), [MergeTree on S3](https://altinity.com/blog/clickhouse-mergetree-on-s3-intro-and-architecture), [mutations vs lightweight deletes](https://oneuptime.com/blog/post/2026-03-31-clickhouse-mutations-vs-lightweight-deletes/view), [avoid mutations](https://clickhouse.com/docs/best-practices/avoid-mutations).

#### Apache Doris and StarRocks
Both are classic LSM-tree OLAP engines: a row-oriented, PK-sorted MemTable (Doris default 200MB) collects writes and flushes into a Rowset of one or more immutable columnar Segment files (Doris Segment V2: 64KB per-column pages with ordinal index, ZoneMap, optional Bloom filter, short-key prefix index). Both resolve primary-key deduplication at write time (Merge-on-Write), not read time: Doris flips old row IDs in a per-(rowset, segment, version) Roaring-bitmap Delete Bitmap and writes the new row to a fresh Rowset; StarRocks tracks deleted row IDs in a per-segment DelVector. Deleted rows stay physically on disk, invisible only via bitmap/vector filtering, until compaction reclaims them — a row-grain, segment-scoped tombstone in both systems, never cell-grain. Freshness follows commit/transaction boundaries: deduplication becomes visible once a transaction commits, giving analytics the latest per-PK state without read-time aggregation, but unflushed MemTable rows are explicitly excluded from concurrent queries — the isolation boundary sits at rowset/commit, not at row-by-row memtable visibility. Both vendors document an explicit three-way tradeoff between compaction resources, freshness, and query latency, and neither system's docs describe an Arrow-native memtable — Arrow only appears post-flush, via Flight integration. Sources: [Doris load internals](https://doris.apache.org/docs/4.x/data-operate/import/load-internals/), [Doris storage layer design](https://medium.com/@VeloDB_poweredby_ApacheDoris/introduction-to-apache-doris-storage-layer-design-explanation-of-storage-structure-design-83f083acddce), [StarRocks primary key table](https://docs.starrocks.io/docs/table_design/table_types/primary_key_table/), [Doris data update/delete](https://doris.apache.org/docs/dev/key-features/data-update-delete/), [Doris unique key](https://doris.apache.org/docs/dev/key-features/unique-key/), [StarRocks PK best practices](https://docs.starrocks.io/docs/best_practices/primarykey_table/), [StarRocks bulk ingestion](https://www.starrocks.io/blog/escaping-the-small-file-trap-how-starrocks-optimizes-bulk-ingestion), [Doris compaction](https://doris.apache.org/blog/Understanding-Data-Compaction-in-3-Minutes/), [Doris realtime deep dive](https://www.velodb.io/blog/apache-doris-excels-olap-deep-dive-realtime), [StarRocks vs Doris comparison](https://medium.com/starrocks-engineering/detailed-comparison-between-starrocks-and-apache-doris-81ddd34be527).

#### GreptimeDB (Mito engine)
Mito is an LSM-tree engine organized around per-table "regions," each owning its own memtables, Parquet SSTs, and manifest, tuned for narrow-time-range / selected-series / selected-column analytical reads ([storage engine docs](https://docs.greptime.com/contributor-guide/datanode/storage-engine/), [2022 design blog](https://www.greptime.com/blogs/2022-12-21-storage-engine-design)). Its default memtable, `time_series`, is columnar from the moment data lands: writes are keyed by encoded primary key into a `BTreeMap<Vec<u8>, Arc<RwLock<Series>>>`, with each `Series` holding per-column buffers (timestamp, `__sequence`, `__op_type`, fields) rather than row blobs — a redesign the [Mito v2 blog](https://greptime.com/blogs/2023-10-30-mito-engine) says cut memory >10x versus an earlier row-oriented BTreeMap approach. A second, more write-optimized `partition_tree`/merge-tree memtable is documented ([config docs](https://docs.greptime.com/user-guide/deployments-administration/configuration/)) but was not found as a file in the current `main` source tree during this research (2026-07-05), suggesting active churn — treat its exact status as unverified. The columnar boundary is therefore memtable-internal, not flush-time: scans convert the internal `Batch` type to Arrow `RecordBatch` via `BatchToRecordBatchAdapter` in [`memtable.rs`](https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/memtable.rs), and flush simply persists that same columnar shape as immutable, PK+time-sorted Parquet files. Deletes are row-granular only: a per-row `__op_type` (Put/Delete) enum, reconciled against `__sequence` during merge-scan dedup ([`read/dedup.rs`](https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/read/dedup.rs)) — there is no cell-level or range-tombstone analogue. `ScanRegion` ([`read/scan_region.rs`](https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/read/scan_region.rs)) always merges live memtables with on-disk SSTs, so analytics queries see unflushed writes with no staleness window. For a Cassandra coordinator-native OLAP precedent, the key weaknesses are: (1) delete fidelity tops out at whole-row Put/Delete — nothing maps cleanly onto Cassandra's per-cell tombstones, range tombstones, or TTL expiry; (2) merge semantics operate at `(primary_key, timestamp, sequence)` grain, coarser than Cassandra's per-cell LWW; (3) the dual-format cost is real but mitigated — ingest is row-wise `KeyValue` but immediately fanned into columnar per-series buffers, so it avoids a full row→columnar rewrite at flush, unlike engines that stay row-oriented in-memory until flush.

Sources: https://docs.greptime.com/contributor-guide/datanode/storage-engine/ ; https://docs.greptime.com/user-guide/deployments-administration/configuration/ ; https://www.greptime.com/blogs/2022-12-21-storage-engine-design ; https://greptime.com/blogs/2023-10-30-mito-engine ; https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/memtable.rs ; https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/memtable/time_series.rs ; https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/memtable/builder.rs ; https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/read/dedup.rs ; https://github.com/GreptimeTeam/greptimedb/blob/main/src/mito2/src/read/scan_region.rs

#### Tonbo
Tonbo (github.com/tonbo-io/tonbo) is a Rust embedded LSM-style "merge-tree" database purpose-built for serverless/edge compute: writes go WAL → MemTable → Parquet SSTable, coordination is via a manifest committed with compare-and-swap on object storage (S3/R2/MinIO), and compute is stateless. Its most distinctive trait relative to CQLite's plugin design is that the columnar boundary sits at the *front* of the write path, not the back: even the mutable memtable (`src/inmem/mutable/memtable.rs`) stores data as Arrow `RecordBatch`es in pre-allocated write-once slots, indexed only by a `crossbeam-skiplist::SkipMap<KeyTsOwned, DynMutation<BatchRowLoc, DeleteRowLoc>>` keyed on `(key, commit_ts)` — there is no typed-row intermediate representation to convert out of. Deletes are represented as a distinct `DynMutation::Delete` variant carrying a `DeleteRowLoc` into a separate "delete sidecar" (`DeleteSidecar`: a keys-only `RecordBatch` + commit-timestamp vector, `src/inmem/immutable/memtable.rs`), reconciled into an `_tombstone` (`MVCC_TOMBSTONE_COL`) marker column at merge time — this is strictly whole-row (primary-key) tombstoning; the model has no concept of Cassandra-style range, cell, or partition-level deletes. Scans return zero-copy `RecordBatch`es with projection/filter pushdown and support MVCC snapshot/time-travel reads, and per the README's `02_transaction` example explicitly guarantee "read-your-writes," i.e. unflushed memtable data is live to analytical scans (verified, not inferred). For a Cassandra coordinator-native OLAP precedent, the relevant gaps are: it is a single-process **embedded** library (no multi-node/distributed write path — CAS-on-manifest gives optimistic-concurrency safety for competing writers, not clustered coordination), it is pre-1.0/alpha (crates.io `0.4.0-a1`, "early stages" per Tonbo's own blog), and its delete model — row-grain-only — is materially simpler than Cassandra's tombstone taxonomy, so any adapter would need an additional reconciliation layer for range/cell/partition deletes rather than reusing Tonbo's sidecar as-is.

Sources:
- https://github.com/tonbo-io/tonbo
- https://github.com/tonbo-io/tonbo/blob/main/README.md
- https://github.com/tonbo-io/tonbo/blob/main/src/inmem/mutable/memtable.rs
- https://github.com/tonbo-io/tonbo/blob/main/src/inmem/immutable/memtable.rs
- https://github.com/tonbo-io/tonbo/blob/main/src/mutation.rs
- https://tonbo.io/blog/introducing-legacy-tonbo
- https://tonbo-io.github.io/tonbo/
- https://docs.rs/tonbo/latest/tonbo/
- https://crates.io/crates/tonbo/0.1.0

#### Apache HoraeDB (formerly CeresDB)
HoraeDB's legacy (and only fully-shipped) storage engine is a classic per-table LSM: writes go to a RocksDB/Kafka/OceanBase-backed WAL, then into an active per-table [skiplist memtable](https://github.com/apache/horaedb-docs) based on agatedb's Arena-bounded skiplist, which becomes read-only and is flushed by a background thread into segment-bounded [Parquet SSTs](https://raw.githubusercontent.com/apache/horaedb-docs/main/content/en/docs/design/storage.md). The [architecture doc](https://raw.githubusercontent.com/apache/horaedb-docs/main/content/en/docs/design/architecture.md) is explicit that the `Analytic Engine`'s table read path sources data from "SST **and** Memtable" together — i.e., unflushed writes are visible to analytical queries immediately, which is the freshness property CQLite's coordinator-native design also wants. Compaction (explicitly modeled on Cassandra's STCS/TWCS) merges small SSTs within a segment and is also where expired/duplicate data is dropped; there is no row-level DELETE or tombstone in either the SQL layer or the storage code inspected in `apache/horaedb` — HoraeDB is schema'd time-series-first, so "delete" only exists as coarse, segment/TTL-driven file purge during compaction, never per-row/per-cell. Notably, HoraeDB's abandoned 2024–2025 rewrite (`main` branch, RFC [`20240827-metric-engine.md`](https://github.com/apache/horaedb/blob/main/docs/rfcs/20240827-metric-engine.md), never reached GA before the podling retired) went further and eliminated the memtable altogether — its `ColumnarStorage::write()` takes an Arrow `RecordBatch` directly and writes it as a brand-new Parquet SST per call (`src/columnar_storage/src/storage.rs`), relying entirely on background compaction to fix up the resulting small-file explosion. Query execution throughout is DataFusion/Arrow-native (RecordBatch streams), confirmed via the [architecture doc](https://raw.githubusercontent.com/apache/horaedb-docs/main/content/en/docs/design/architecture.md) and the DataFusion project's own list of downstream users. For a Cassandra coordinator-native OLAP design, HoraeDB is a cautionary rather than a positive precedent: it never supports Cassandra-style per-cell tombstones/overwrites (append+TTL-purge only, unsuited to a delete/update-heavy CQL workload), its memtable model is single-node/single-table (no notion of merging tails across peer replicas the way a coordinator-side scan engine would need), and the project retired from Apache incubation in March 2026 before its more relevant memtable-less/direct-columnar rewrite matured — that rewrite is nonetheless a real data point that "skip the memtable, buffer straight into columnar files" was seriously attempted by an LSM/time-series project, just not proven at scale.

Sources:
- https://raw.githubusercontent.com/apache/horaedb-docs/main/content/en/docs/design/storage.md
- https://raw.githubusercontent.com/apache/horaedb-docs/main/content/en/docs/design/architecture.md
- https://github.com/apache/horaedb (main-branch source, incl. `src/columnar_storage/`, `src/metric_engine/`)
- https://github.com/apache/horaedb/blob/main/docs/rfcs/20240827-metric-engine.md
- https://github.com/apache/horaedb/blob/main/README.md
- https://incubator.apache.org/projects/horaedb.html (podling retired 2026-03-03)
- https://dbdb.io/db/horaedb

#### RocksDB, ScyllaDB, HBase, SlateDB (OLTP contrast group)
All four are classic LSM-trees — RocksDB and SlateDB LevelDB-derived, ScyllaDB and HBase from the Cassandra/BigTable lineage — and all four keep their memtable strictly row-oriented: RocksDB's default SkipList, ScyllaDB's red-black tree with MVCC, HBase's `ConcurrentSkipListMap`-backed `AbstractMemStore`, and SlateDB's SkipList-based dual-memtable design. None expose any columnar boundary at all inside the core storage path — SSTables persist as row-oriented block tables or partitions, and turning that data columnar for analytics requires a separate ETL/export step entirely outside the engine. Delete grain is standard LSM tombstoning: RocksDB point (`Delete`/`SingleDelete`) and range (`DeleteRange`) tombstones reclaimed only at bottommost compaction; ScyllaDB/HBase (and Cassandra itself) use cell/row/partition tombstones with a `gc_grace_seconds` retention window (default 10 days) during which accumulating droppable tombstones degrade read latency. Freshness across all four follows MVCC snapshot isolation via sequence numbers, but the unflushed memtable is coordinator/node-local and invisible to distributed analytics reads — a query must wait for flush, and for tombstone-heavy data, for compaction, before the analytical view matches the true source-of-truth state. Sources: [RocksDB MemTable wiki](https://github.com/facebook/rocksdb/wiki/MemTable), [RocksDB SST formats](https://github.com/facebook/rocksdb/wiki/A-Tutorial-of-RocksDB-SST-formats), [RocksDB CompactionFilter](https://github.com/facebook/rocksdb/wiki/Compaction-Filter), [RocksDB Snapshot](https://github.com/facebook/rocksdb/wiki/Snapshot), [RocksDB internals](https://medium.com/@ghufrankhan_921/understanding-rocksdb-internals-lsm-trees-memtables-sstables-and-compaction-5cba4138de71), [ScyllaDB internal cache](https://www.scylladb.com/2024/01/08/inside-scylladbs-internal-cache/), [ScyllaDB in-memory](https://docs.scylladb.com/stable/using-scylla/in-memory.html), [SSTables 3.0 format](https://github.com/scylladb/scylladb/wiki/SSTables-3.0-Data-File-Format), [ScyllaDB workload coexistence](https://www.scylladb.com/2026/01/28/can-database-workloads-coexist/), [tombstone cost](https://datanised.com/2025/07/01/part-1-the-hidden-cost-of-deletion-how-tombstones-work-in-scylladb/), [Cassandra tombstones doc](https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/tombstones.html), [HBase MemStore internals](https://pierrezemb.fr/posts/diving-into-hbase-memstore/), [HBase MemStore API](https://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/regionserver/MemStore.html), [SlateDB design overview](https://slatedb.io/docs/design/overview/), [SlateDB repo](https://github.com/slatedb/slatedb), [row vs columnar](https://dev.to/alexmercedcoder/columnar-vs-row-based-data-structures-in-oltp-and-olap-systems-20c8), [LSM survey arXiv](https://arxiv.org/pdf/2101.06801), [VLDB LSM paper](http://vldb.org/pvldb/vol14/p1872-yan.pdf).

### 3. The Two Universal Lessons

**Lesson 1 — nobody mutates columnar data in place; everyone seals immutable segments from a mutable staging structure.** This holds without exception across all nine profiles, though the staging structure isn't always literally "row/tree." Kudu is the cleanest case: encoded columnar CFile data is explicitly documented as infeasible to mutate in place, so all post-flush changes are shunted into a wholly separate delta B-tree. Paimon, Doris/StarRocks, and the RocksDB/Scylla/HBase/SlateDB group all buffer in a row-oriented, key-sorted structure (sort buffer, MemTable, skiplist) and only ever *create new* immutable columnar/SSTable files at flush — never rewrite an existing one in place; ClickHouse skips the staging structure but still never mutates a sealed part, relying on new-part-plus-merge instead. InfluxDB/IOx is the instructive partial exception: it converts to Arrow `RecordBatch`es unusually early (~1s after a brief row-form WAL op-log), but even those Arrow batches are only append-accumulated and are still sorted/deduped/rewritten wholesale into a *new* immutable Parquet file at snapshot time — the in-memory columnar buffer is never mutated cell-by-cell either. The three backfilled systems sharpen the same point: GreptimeDB's `time_series` memtable is columnar per-series *append* buffers from the moment data lands; Tonbo's mutable memtable is literally write-once Arrow `RecordBatch` slots under a skiplist MVCC index — "mutation" is a new version, never an in-place edit; and HoraeDB's abandoned rewrite dropped the memtable entirely to write a new Parquet file per `RecordBatch`, relying on compaction to absorb the small-file explosion — attempted, never proven at scale (podling retired 2026-03). So the honest form of the lesson is: **immutable-segment-from-mutable-buffer is universal; what varies is only how early the buffer becomes columnar, never whether the sealed segment gets mutated.**

**Lesson 2 — systems needing OLTP-style point reads/updates keep memory row-oriented; systems that don't, skip the row stage entirely.** Kudu (MemRowSet), the RocksDB/Scylla/HBase/SlateDB group (skiplists/trees), Paimon (PK-sorted buffer), and Doris/StarRocks (MemTable) all support point lookups/upserts and all stay row-oriented until flush — Kudu's own docs make the mechanism explicit ("data in columnar engines must be decoded, updated, re-encoded"), which is precisely why point-mutation workloads avoid columnar-in-memory. ClickHouse is the clean counter-case that proves the rule rather than breaking it: it has *no* memtable at all, because it has no point-update requirement to serve — pure append/insert-then-merge lets it flush straight to columnar. IOx sits in between: it serves point-ish, tag-scoped queries but converts to columnar within ~1 second anyway, suggesting the row stage's necessity scales with how latency-sensitive point access is, not a strict binary. Revised lesson: **the row-oriented memory stage exists to serve point access cheaply, not because in-memory columnar is technically impossible — drop the point-access requirement (ClickHouse) or tolerate a short columnar-conversion lag (IOx) and the row stage shrinks or disappears.**

### 4. Weaknesses in Prior Art This Design Overcomes

**Delete/tombstone fidelity through the columnar boundary.** No system surveyed carries cell-grain LWW, range tombstones, or TTL semantics through its columnar boundary intact. Paimon's Deduplicate engine deletes the whole row for a PK, and its one field-grain engine (Partial Update) refuses deletes by default and can never express "set this cell to NULL" — incoming NULLs are simply never applied. Kudu tracks deletes at (rowid, timestamp) grain via REDO/UNDO — closer than most, but still not a per-cell tombstone independent of the row. Doris and StarRocks resolve deletes as a (rowset, segment[, version]) bitmap/vector — segment-scoped, row-grain, with no notion of an individual column surviving while its row is marked deleted. ClickHouse fragments the problem entirely across engines (version rows, sign-pair collapse, mutation-rewrite, lightweight mask) with no unified semantics. RocksDB/Scylla/HBase/SlateDB use point/range/partition tombstones at the classic LSM grain, with a documented read-latency cost while droppable tombstones accumulate pre-compaction. The backfilled trio confirms the ceiling: GreptimeDB tops out at whole-row `__op_type` Put/Delete reconciled by `(primary_key, timestamp, sequence)`; Tonbo at whole-row PK tombstones in a delete-sidecar `RecordBatch`; HoraeDB has no row-level delete at all — only file-grain TTL purge at compaction. CQLite's cell-level, tombstone-correct k-way LWW merge — matching actual CQL/Cassandra per-cell writetime semantics — is a materially stronger fidelity guarantee than anything in this survey ships today.

**Freshness of unflushed data to analytics.** The survey splits cleanly. The batch/lake/warehouse systems treat the unflushed write buffer as invisible to analytical reads: Paimon is hard-bound to Flink checkpoint cadence (1–10 min) with zero read path into the writer's buffer; ClickHouse is stale until background merge completes, forcing a 10x+ `FINAL` penalty for correctness; Doris/StarRocks exclude MemTable rows from concurrent queries by isolation-boundary design; and the RocksDB/Scylla/HBase/SlateDB group keeps the unflushed memtable strictly node-local and invisible to distributed reads. The analytics-native engines — IOx, Kudu, GreptimeDB, Tonbo, HoraeDB — all serve unflushed data live, but in every case via an *engine-internal* merge inside a single process: IOx pays a per-query RPC fan-out to every relevant ingester (Clustered) or an unbounded query-time dedup cost with no compactor at all (Core); Kudu's guarantee holds only inside a single tablet; GreptimeDB, Tonbo, and HoraeDB merge their own memtables into their own scans. None of them exposes the live buffer *across a process/language boundary to a foreign scan engine* — which is exactly the seam a Cassandra coordinator-native design must cross, and what the sanctioned-iteration tail handoff (§4) is for. The design goal — OLAP freshness equal to the engine's own read freshness — is thus well-precedented; achieving it across the JVM→engine boundary with full Cassandra delete semantics is not.

**Dual-format maintenance cost.** Systems that materialize a genuinely separate second format pay for it visibly at compaction: Kudu's DiskRowSet base columns are immutable-once-encoded, forcing every update into a separate DeltaMemStore/DeltaFiles structure that must be scanned in full on every read of a hot row, with no predicate pushdown to skip it. Doris and StarRocks both pay an unavoidable row-to-columnar transcode at every flush with no way to skip the encode phase, and neither's memtable is Arrow-native — Arrow only appears post-flush via Flight. IOx, despite having "one" durable format (Parquet), still carries three live representations reconciled per query (WAL op-log, in-memory Arrow buffer, multi-generation Parquet), and its Clustered tier adds an external Postgres-compatible catalog service as a correctness-critical dependency. A design that treats Arrow segments purely as an invalidated-at-compaction cache — never a semi-durable tier with its own generations — avoids this reconciliation surface entirely.

**Merge-semantics grain.** Paimon's per-key merge ordering is a `_SEQUENCE_NUMBER` (input-arrival-derived, or a schema-time-declared `sequence.field`/`sequence-group`) — a row- or declared-field-group grain, never an automatic independent timestamp per cell the way Cassandra assigns a writetime to every column write by default. Doris/StarRocks resolve merge purely as Merge-on-Write bitmap/vector filtering at rowset/segment grain, with no per-column reconciliation concept at all. RocksDB-family LSMs merge whole level-N files against level-(N+1), with no ability to redirect merge output to an analytics-optimized grain or format. None of the nine systems perform anything resembling Cassandra's per-cell reconciliation — the survey confirms that "row sequence number" (Paimon, Doris/StarRocks) or "whole-file/whole-level" (the classic LSM group) are the only merge grains in production prior art, leaving CQLite's cell-grain k-way LWW merge as a genuinely novel contribution rather than a refinement of an existing pattern.

---

## 4. The tail handoff: format, freshness, deletes

### 4.1 Format: Cassandra-native `nb` serialization, never Arrow-with-envelope

The tail crosses the JVM→engine boundary in **Cassandra's own serialization**, produced by the same machinery a real flush uses (`getFlushSet` + `SSTableTxnWriter` in the snapshot mode; the same row serializer in the stream mode). Consequences:

- **Correct by construction on both ends** — Cassandra's own code writes the format; CQLite's parity-tested `nb` decoder reads it. No second serializer covering every CQL type in the plugin JVM; no new parity surface.
- **Deletes ride along natively.** A delete *is* data — a tombstone in the memtable with its own timestamp, at cell / row / range / partition grain. All four grains serialize exactly as flush writes them; CQLite's merge already reconciles them:

  ```
  flushed SSTable (gen 12):   row x = 1        @ t1
  tail:                       tombstone(x)     @ t2   (unflushed delete)
  merge result:               x suppressed  ✓  (t2 > t1)
  ```

- **The envelope problem vanishes from the tail.** An Arrow-format tail would need per-cell writetimes, TTL/localDeletionTime, per-element collection writetimes/cell-paths, and a separate range-tombstone representation — the hard 20%, and precisely what every naive Parquet proposal gets wrong the moment the newest write is a delete. Arrow appears only post-merge, where the data is clean. (The envelope survives in exactly one optional place: the per-generation *cache* segments, §6, where it lets the merge itself run columnar.)
- **Ruled out: raw Rust reads of the trie's off-heap memory.** `InMemoryTrie`'s buffer layout is an undocumented, version-specific internal; a foreign reader cannot participate in the `readOrdering` barrier lifecycle that keeps flush from freeing buffers under it. That is a no-heuristics violation at the memory level. The sanctioned door is the JVM-side iteration API — same door flush uses.

### 4.2 Freshness ladder: snapshot (v1) → live stream (v2)

Both modes use the **same sanctioned iteration** (`readOrdering`-pinned, token-ordered — so it plugs into the k-way merge exactly like an SSTable cursor). Only the **sink** differs:

| | v1 — snapshot file (approved #1807 design) | v2 — per-query live stream |
|---|---|---|
| Artifact | real `nb` SSTable, staging dir → **atomic rename** into tail dir | native-serialization stream over UDS/shared memory; no file |
| Freshness trigger | on-demand: stale read drops a request marker; export only if `operationCount()` advanced **and** min-interval elapsed | iterator opens **at query time** |
| What a scan sees | everything up to the export point (commit-log-interval **watermark in Statistics.db** says exactly what — no inference) | everything in the memtable at iterator-open = **CL.ONE read freshness** |
| Cost | first stale read pays export latency (flush-equivalent, ~sub-second–seconds); burst amortized | per-query trie iteration + serialization; pin held for stream lifetime |
| Failure | passive artifact, crash-safe, `sstabledump`-debuggable | per-query protocol; retry = re-open iterator |

**Acked-write → OLAP-visible latency** (the question that decides the product story):

| Mode | Ack → visible | Pay |
|---|---|---|
| v2 live-stream | **next query** (= CL.ONE CQL read freshness) | per-query iteration; replication lag still applies |
| v1, export-eligible | that same query | + export latency on first stale read |
| v1, inside min-interval | ≤ min-interval (config, ~seconds) | nothing — accepted bounded staleness |
| any mode, laggard replica scanned | replication lag (ms in-DC; unbounded under failure until hints/repair) | inherent to Cassandra — identical exposure to a CL.ONE read |

**Headline:** this model's OLAP freshness is bounded below by Cassandra's own replication, and v2 achieves that bound. (End-to-end paper simulations of every direction, with per-stop latency trees and the freshness-vs-latency frontier: §12.) The state of the art (Spark bulk reader) is stale by "whenever you last took a snapshot" — minutes to hours. Within one query the tail is snapshot-at-iterator-open (like Cassandra's own range reads); a single mutation never half-appears — the iterator yields reconciled partitions, not raw cells.

**v2's honest costs** (→ WS2/WS8): standing read pressure on the write path's hot structure (re-bench #1807's G3 for this mode); pin duration delays the flushed memtable's buffer reclamation → chunked streaming + automatic snapshot-fallback above a size/duration threshold; a stream doesn't survive a mid-scan engine crash (retry protocol).

**Correctness backstop (both modes):** an orphaned stale export must not outlive `gc_grace` — a tail file surviving past tombstone purge on the main line can resurrect data. Gets a test, not a comment (#1807 design flags this).

---

## 5. Cassandra Java-side requirements (CEP-11 plugin)

*Compiled from `docs/storage engine/memtable-plugin-design.md` (design, spike #1807), `docs/storage engine/cassandra-index/research-plugin-mechanics.md` (mechanics research), `docs/storage engine/report-1-memtable-freshness.md`, `docs/storage engine/report-2-storage-engine-feasibility.md`, plus a skim of `cassandra-index/research-export-format.md`, `research-cqlite-tail-seam.md`, `memtable-api.md`, `memtable-pool-allocator-backpressure.md`, `sai-live-memtable-index.md`, `cfs-flush-lifecycle.md`. All anchors are on `origin/cassandra-5.0` @ `464b2e54` unless marked `[trunk]`.*

---

### 1. Why extend, not wrap — the two `instanceof AbstractAllocatorMemtable` gates

`CqliteMemtable` **must extend `TrieMemtable`** (inheritance), not compose/wrap it. A composition wrapper (`implements Memtable`, holding an inner `TrieMemtable`) is fatally broken by two hardcoded `instanceof` checks that cannot be fixed from outside the wrapper:

1. **Memory-pressure flush selection skips wrappers.** `AbstractAllocatorMemtable.flushLargestMemtable()` (`AbstractAllocatorMemtable.java:249-318`) iterates each CFS's *current* memtable via `ColumnFamilyStore.activeMemtables()` (`ColumnFamilyStore.java:1447-1452` — returns the wrapper, never the inner) and at **`AbstractAllocatorMemtable.java:260-261`**:
   ```java
   if (!(currentMemtable instanceof AbstractAllocatorMemtable)) continue;
   ```
   A wrapper's inner allocator's memory counts against the global pool, but the wrapper itself is invisible to the cleaner — if the plugin table is the largest consumer, the cleaner flushes the wrong table or nothing, and writes stall on pool backpressure with no relief.
2. **Periodic flush (`memtable_flush_period_in_ms`) is also instanceof-gated**: `AbstractAllocatorMemtable.java:218-220` only acts `if (current instanceof AbstractAllocatorMemtable)`.

A third, related break: the **flush-signal identity mismatch** — the inner memtable calls `owner.signalFlushRequired(this /* = inner */, MEMTABLE_LIMIT)`, but `CFS.signalFlushRequired → switchMemtableIfCurrent` compares by **reference identity**: `if (data.getView().getCurrentMemtable() == memtable)` (`ColumnFamilyStore.java:1014-1022`). The current memtable is the wrapper, not the inner, so the switch silently never fires unless composition supplies a delegating `Owner` that rewrites the identity (`research-plugin-mechanics.md` §2 point 2, §7).

Composition additionally requires forwarding ~30 methods (writes/reads, stats, memory, flush, lifecycle — full enumeration in `research-plugin-mechanics.md` §2 "Full forwarding surface") and a delegating `Owner`, and *still* loses both gates. Extension gets pool registration, cleaner eligibility, flush signaling, and commit-log bookkeeping for free (`research-plugin-mechanics.md` §2 "What extension gets for free"; §7 "MemtablePool safety").

---

### 2. Package-private constructor → split-package requirement

`TrieMemtable`'s constructor is **package-private**: `TrieMemtable(AtomicReference<CommitLogPosition>, TableMetadataRef, Owner, Integer shardCountOption)` at **`TrieMemtable.java:123`**, as is `AbstractShardedMemtable`'s (`AbstractShardedMemtable.java:56`). `TrieMemtable` itself is `public class TrieMemtable extends AbstractShardedMemtable` (`TrieMemtable.java:89`) — not `final` — but the constructor forces the subclass to live in **package `org.apache.cassandra.db.memtable`, inside the plugin jar**.

This is legal on 5.0's flat classpath (no JPMS sealing) but is a **split package** — risk #1 in `research-plugin-mechanics.md`'s "Risks / unknowns": *fragile against future JPMS modularization*. Mitigation held by the design: keep **only** the thin `CqliteMemtable` subclass + its static `factory(Map)` method in `org.apache.cassandra.db.memtable`; put **all** export logic (the exporter, writer recipe, handshake, GC) in a separate `cqlite.*` package, referenced from the thin subclass (`memtable-plugin-design.md` §4.1, code sample lines 109-150).

Note `AbstractAllocatorMemtable.initialFactory` is read from `metadata().params.memtable.factory()` at construction (`AbstractAllocatorMemtable.java:120`) — i.e. **our own factory** — so `shouldSwitch(SCHEMA_CHANGE)` comparisons remain correct without an override, provided the factory implements `equals`/`hashCode` (see §3).

---

### 3. Exact API surface used

#### Factory contract
- **Interface**: `Memtable.Factory` at `Memtable.java:75-152`; single required method (`Memtable.java:86`): `Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadaRef, Owner owner)`.
- **Optional factory defaults** (all default `false`/`null`): `writesShouldSkipCommitLog()` (`:95`), `writesAreDurable()` (`:108`), `streamToMemtable()` (`:121`), `streamFromMemtable()` (`:137`), `createMemtableMetrics(TableMetadataRef)` (`:148`).
- **Reflection path**: `MemtableParams.getMemtableFactory` (`schema/MemtableParams.java:217-257`) — `Class.forName(className)` → `getDeclaredMethod("factory", Map.class)` invoked with a **mutable copy** of parameters; falls back to a static `FACTORY` field on `NoSuchMethodException`. **Parameter-consumption contract**: `factory(Map)` must `map.remove(...)` every option it owns; a non-empty map afterward → `ConfigurationException` (`MemtableParams.java:246-248`).
- **equals/hashCode**: the factory instance **must implement both** — used by `AbstractAllocatorMemtable.shouldSwitch(SCHEMA_CHANGE)` to detect memtable-type changes (`AbstractAllocatorMemtable.java:137`); `TrieMemtable.Factory` implements both at `TrieMemtable.java:681-698` as the pattern to copy. `CqliteFactory` in the design (`memtable-plugin-design.md` line 148) implements this.
- Instantiation is **lazy**: a bogus yaml configuration does not fail node startup, only first table reference (`MemtableParams.java:111-120`).

#### `getFlushSet`
`Memtable.getFlushSet(PartitionPosition from, to)` (`Memtable.java:303`; `TrieMemtable.java:350-403` impl) returns a live `FlushablePartitionSet` — a subtrie view of the live `mergedTrie` — that does **NOT** require `switchOut` to have happened first (`research-export-format.md` §1.2). It does one O(partitions) key-count/size pre-pass up front (`TrieMemtable.java:353-366`, `:356-366`) before returning the transforming iterator. It never calls `setFlushTransaction`, so a concurrent real flush is neither blocked nor double-flush-tripped (`Flushing.java:61-63`). Supports token-range chunking natively (`getFlushSet(from, to)`).

#### `switchOut` / `discard`
- `switchOut(OpOrder.Barrier, AtomicReference<CommitLogPosition> upper)` (`AbstractAllocatorMemtable.java:162` declared, `Memtable.java:354` interface) — export cut point: nothing new arrives after the write barrier issues. **Never block inside `switchOut`** — it runs under the flush path; schedule export finalization on an executor instead.
- `discard()` (`TrieMemtable.java:161` / `AbstractAllocatorMemtable.java:168`) — the flushed sstable is now live on disk, so this memtable's exports are retirable (GC hook).
- Lifecycle callback order on flush switch (`ColumnFamilyStore.Flush` ctor `:1175-1218`, `Flush.flushMemtable` `:1290-1390`): (1) `createMemtable` via our factory; (2) `Tracker.switchMemtable` swaps the view, fires `MemtableSwitchedNotification` (`lifecycle/Tracker.java:559`); (3) `oldMemtable.switchOut(writeBarrier, commitLogUpperBound)` (`:1213`); (4) `setCommitLogUpperBound` seals a `LastCommitLogPosition` CAS (`:1418-1432`), then `writeBarrier.issue()`; (5) `Flushing.flushRunnables` → `getFlushSet` per disk region → sstables written; (6) `cfs.replaceFlushed` then `reclaim(memtable)`: `readOrdering` barrier → await → `memtable.discard()` (`:1391-1405`), `Tracker` fires `MemtableDiscardedNotification` (`Tracker.java:564`). `getFinalCommitLogUpperBound()` is valid from step 4 onward.

#### `shouldSwitch` / `performSnapshot`
- `shouldSwitch(FlushReason)` (`AbstractAllocatorMemtable.java:131` — `default: return true` for every reason except SCHEMA_CHANGE-without-change and OWNED_RANGES_CHANGE, `:131-143`). Stock memtables therefore flush-before-snapshot always; `performSnapshot` is never called for them (default impl throws `AssertionError`, `:157-160`).
- `performSnapshot(name)` (`AbstractAllocatorMemtable.java:157` declared) fires **only** when the memtable overrides `shouldSwitch(SNAPSHOT) → false` — i.e. live, current, pre-flush, writes still flowing (`ColumnFamilyStore.snapshot`, `ColumnFamilyStore.java:2375-2390`). Sole caller on 5.0; **no `TakeSnapshotTask` exists on 5.0** (trunk-only relocation, `[trunk] service/snapshot/TakeSnapshotTask.java:136`). Side effect if overridden: snapshots **stop containing memtable data as sstables** unless the snapshot tooling also consumes the tail export — this is design NEEDS-DECISION (a), recommendation is to keep stock behavior for the spike (`memtable-plugin-design.md` §10(a)).
- With `nodetool snapshot --skip-flush`, the memtable is not consulted at all — confirmed: snapshots do NOT capture the memtable tail via `skipMemtable` path → `snapshotWithoutMemtable` (`ColumnFamilyStore.java:2124-2160,2375-2389`; `research-export-format.md` §5).

#### `operationCount()` — dirty-check
`Memtable.operationCount()` (`Memtable.java:213`) is the dirty-check signal: the plugin exports **only if** `operationCount()` has advanced since the last export **and** the min-interval cap has elapsed (`memtable-plugin-design.md` §4.3 step 2). A clean memtable or too-recent export answers a request without republishing.

#### `readOrdering` pin mechanics — why iteration without it is unsafe
After a real flush, `Flush.reclaim(memtable)` issues `readOrdering.newBarrier()`, awaits it, **then** calls `memtable.discard()` which frees the trie's off-heap buffers (`ColumnFamilyStore.java:1391-1405`; `TrieMemtable.discard():161-181`; off-heap free via `NativeAllocator.setDiscarded()` → `MemoryUtil.free`, `utils/memory/NativeAllocator.java:200-205`). **Iterating without holding a read-ordering group risks reading freed off-heap memory.** The exporter must run its entire iteration inside:
```java
try (OpOrder.Group op = cfs.readOrdering.start()) { ... getFlushSet + write ... }
```
(`ColumnFamilyStore.java:305`, pattern precedent at `:2042`; normal reads use the same mechanism via `ReadExecutionController.forCommand`, `db/ReadExecutionController.java:129-153`). `Owner` *is* the CFS on 5.0 but does not expose `readOrdering`, so **casting `owner` to `ColumnFamilyStore`** is an accepted implementation coupling (risk #3, `research-plugin-mechanics.md`). Consequence: a slow export **delays off-heap reclaim of a concurrently-flushed memtable** — bound export time or chunk by token range (`getFlushSet(from,to)` supports ranges natively).

A second, separate live-iteration hazard is documented in-tree: `FlushablePartitionSet`'s own javadoc warns a still-written memtable may violate collected encoding stats / column sets (`Memtable.java:308-311`). Mitigation: build the `SerializationHeader` with `EncodingStats.NO_STATS` (epoch-based, always safe, `EncodingStats.java:69`) and the table's **full** `regularAndStaticColumns()` rather than `flushSet.columns()`/`flushSet.encodingStats()`. Statistics.db min/max timestamps are unaffected — they're collected from actual cells during `append` (`SortedTableWriter.java:195,214,227-232`; `MetadataCollector.java:222-262`).

Consistency semantics while writes continue (TrieMemtable specifics, `research-plugin-mechanics.md` §3): shards are single-writer-locked (`ReentrantLock writeLock`, `MemtableShard.put`, `TrieMemtable.java:459-503`) but reads are lock-free/concurrent; snapshot granularity is **per-partition**, not cross-partition — a concurrent scan may see partition A pre-update and partition B post-update. There is **no cross-partition point-in-time snapshot**.

---

### 4. The export path

**Writer recipe** — reuses the flush serialization path with an offline transaction (all in-tree 5.0 APIs; `memtable-plugin-design.md` §4.3 code block, `research-export-format.md` §1.1):

```java
CommitLogPosition pStart = CommitLog.instance.getCurrentPosition();   // watermark, BEFORE getFlushSet
FlushablePartitionSet fs = memtable.getFlushSet(minBound, maxBound);  // Memtable.java:303
SerializationHeader header = new SerializationHeader(true, metadata,
    metadata.regularAndStaticColumns(), EncodingStats.NO_STATS, false);
Descriptor desc = cfs.newSSTableDescriptor(stagingDir, BigFormat);    // CFS.java:975-995
SSTableTxnWriter w = SSTableTxnWriter.create(cfs, desc, fs.partitionCount(), UNREPAIRED, null, false, header);
for (Partition p : fs) try (UnfilteredRowIterator it = p.unfilteredIterator()) { w.append(it); }
w.finish(false);   // openResult=false — never opens a reader, never touches the live set
```

- `SSTableTxnWriter.create` (`SSTableTxnWriter.java:111-116`) → `LifecycleTransaction.offline(OperationType.WRITE)` → `Tracker.newDummyTracker()` (`LifecycleTransaction.java:176-180`) — **zero live-set impact**; this is the exact mechanism `CQLSSTableWriter`, streaming, and scrub tools already use to write real SSTables safely.
- `finish(false)` → `SSTableWriter.TransactionalProxy.doPrepare` (`format/SSTableWriter.java:386-393`) prepares data/index/filter writers and writes TOC at prepare (`TOCComponent.updateTOC`, `:390`); `finish(true)` would open a reader — **always use `finish(false)`** (`SSTableTxnWriter.java:103-108`).
- **`commitLogUpperBound` is null/undefined pre-`switchOut`** — the exporter must stamp its own interval via the **8-arg `createSSTableMultiWriter` overload** (`CFS.java:665-672`), not `Flushing.createFlushWriter`: `IntervalSet<>(memtable.getCommitLogLowerBound(), pStart)` (lower-bound getter `Memtable.java:373-375`).
- `Descriptor` id comes from the CFS's own `sstableIdGenerator` — **do not mint ids independently**; collision-safe because export and flush ids share one sequence (`ColumnFamilyStore.java:308,975-995`, `:993-994` asserts `!Data.db exists`).
- **Component set**: follows the writer builders — `DATA, STATS, DIGEST, TOC` (`SortedTableWriter.java:487`) + BIG's `PRIMARY_INDEX, SUMMARY` (`BigTableWriter.java:368-372`) + `FILTER`/`COMPRESSION_INFO`/`CRC` per table params. CQLite needs the **full set** (Summary.db for token pruning, Statistics.db for open-time checks + watermark) — do not suppress components.
- **Rejected alternatives** (`research-export-format.md` §§2, 3, §1.6): `CQLSSTableWriter` mutates global schema on a live node (`Schema.instance.transform(...)`, `CQLSSTableWriter.java:682-732`) and expects CQL-statement input — a bulk-load tool, not usable in-server; Arrow IPC needs a ~10-15 MB shaded dependency stack + `--add-opens` JVM flags + a brand-new CQLite tail reader, and loses free Statistics.db watermark metadata; `UnfilteredRowIteratorSerializer` is MessagingService-versioned with no cross-release stability (`db/rows/UnfilteredRowIteratorSerializer.java:15-42`).

**Atomic publication + staging** (`memtable-plugin-design.md` §4.5; `research-export-format.md` §4): Cassandra itself does **not** tmp+rename data files — crash-atomicity comes from the LogFile txn protocol (`LogFile.java:65-67,144,196,343-345`) — but an external Rust reader must never parse txn logs, so atomicity is layered on top:
1. Write into `<export_root>/<ks>/<tableId>/.staging-<seq>/` via the recipe above; the `<ver>_txn_write_<uuid>.log` lives and dies inside staging.
2. fsync the staging dir; `Files.move(staging, final, ATOMIC_MOVE)` → `gen-<seq>-.../`; fsync the parent. Same-filesystem directory rename is atomic.
3. Reader contract: descend only into `gen-*` dirs; optionally verify `Digest.crc32`.

**Commit-log-interval watermark in Statistics.db**: `StatsMetadata.commitLogIntervals` (`io/sstable/metadata/StatsMetadata.java:64`, serialized `:319+`) is the **authoritative** copy of the watermark (the dir-name/manifest copies are for cheap discovery without parsing Statistics.db). Semantics are deliberately **fuzzy-upper**: capture `P_start = CommitLog.instance.getCurrentPosition()` *before* `getFlushSet`; stamp `IntervalSet(memtable.getCommitLogLowerBound(), P_start)`; any live export may additionally contain items newer than `P_start` (harmless under LWW). Exact-cutoff semantics exist only for the final `switchOut`-hook export (`AbstractMemtableWithCommitlog.accepts`, `:69-109`). Flushed-sstable correlation is verified: `Flushing.createFlushWriter` passes the same `IntervalSet` shape (`Flushing.java:203-221`).

**GC / crash-sweep**: on plugin startup, delete all `.staging-*` dirs — Cassandra's own `removeUnfinishedLeftovers` only runs for real data dirs (risk #7, `research-export-format.md`).

---

### 5. On-demand dirty-check + min-interval design and flush hooks

Owner-decided (2026-07-03) trigger revision, superseding an earlier timer-primary design (`memtable-plugin-design.md` §4.3 changelog): blind periodic export has real write amplification — each export rewrites the entire live memtable (flush-equivalent serialization), so at interval T over memtable lifetime L the extra bytes are ~½ × (L/T) × final-size, **~300× at 1s cadence over a 10-minute lifetime**.

Trigger chain:
1. **Query arrival (CQLite side)**: on a Flight query for a tail-enabled table, CQLite checks tail staleness and, if stale, requests an export via the filesystem handshake (§4.4 — an empty `request-<seq>` marker file, watched via `java.nio.file.WatchService`, fallback coarse poll), then waits bounded for a new `gen-*` dir.
2. **Dirty-check (plugin side)**: on a request, export **only if** `operationCount()` has advanced since the last export **and** the `min_export_interval_ms` cap has elapsed. A clean/too-recent memtable answers by touching the existing manifest so the requester's wait completes without republishing.
3. **Interval fallback mode** (`export_mode: interval`): copies `AbstractAllocatorMemtable.scheduleFlush`'s pattern exactly — `ScheduledExecutors.scheduledTasks.scheduleSelfRecurring(...)` (`AbstractAllocatorMemtable.java:204-224`), deliberately capturing the **`Owner`** (not the memtable) and re-resolving `owner.getCurrentMemtable()` each tick to avoid pinning a dead memtable.

Flush hooks are **unchanged by the trigger revision**: `switchOut` marks the export cut point and hands finalization to an executor (never blocks); `discard` means the flushed sstable is live → this memtable's exports are retirable. `performSnapshot` fires only on explicit snapshots (§3 above, NEEDS-DECISION (a)).

Query-latency trade, stated explicitly: the first stale read pays export latency (bounded by memtable size — flush-equivalent serialization + the O(partitions) `getFlushSet` pre-pass) + handshake latency; subsequent reads in the burst serve the published export at zero marginal cost. Config strawmen: `min_export_interval_ms` = 250ms (floor between exports under query storm), `tail_wait_timeout` = 2s (bounded-wait before serving stale) — both NEEDS-DECISION (b), to be swept in the spike's export-latency bench.

---

### 6. Config wiring

**cassandra.yaml** (`memtable-plugin-design.md` §4.2; stock stanza ships at `conf/cassandra.yaml:783-790`):
```yaml
memtable:
  configurations:
    skiplist: { class_name: SkipListMemtable }
    trie:     { class_name: TrieMemtable }
    default:  { inherits: skiplist }
    cqlite:
      class_name: org.apache.cassandra.db.memtable.CqliteMemtable
      parameters:
        export_dir: /var/lib/cassandra/cqlite-tail
        export_mode: on_demand
        min_export_interval_ms: "250"
        export_interval_ms: "1000"     # interval mode only
        shards: "..."                  # forwarded to TrieMemtable
```
Keep the stock `skiplist`/`trie`/`default` entries — overriding the yaml **replaces the whole map**, and `default` must remain resolvable; `expandDefinitions` (`MemtableParams.java:139-205`) only injects `default`→SkipList when `memtable.configurations` is entirely absent (`:141-149`). `InheritingClass.resolve()` merges parent + child parameters, child wins; self-inheritance/loops → `ConfigurationException` (`:158,182`). `MemtableParams.get(key)` (`:111-120`) lazily instantiates and caches per configuration key.

**Per-table DDL**: `CREATE TABLE ... WITH memtable = 'cqlite';` / `ALTER TABLE t WITH memtable = 'cqlite';` / reset via `'default'` (`TableParams.java:315` serializes `AND memtable = '<key>'`). Schema stores only the configuration **key**; class/parameters are node-local yaml, so **heterogeneous rollout is supported by design**.

**Deployment**: drop the jar in `$CASSANDRA_HOME/lib/` — `bin/cassandra.in.sh:53-55` classpath-globs every jar there; **no ServiceLoader, no plugin registry**, pure reflection by class name.

**Option forwarding to TrieMemtable**: the plugin's `factory(Map)` must `CqliteExportConfig.consume(options)` (i.e. `map.remove()` each key it owns); any remaining keys (e.g. `shards`) are forwarded to `TrieMemtable.factory(optionsCopy)` (`TrieMemtable.java:658-663`); `MemtableParams` throws `ConfigurationException` if anything is left unconsumed by anyone in the chain (`MemtableParams.java:246-248`).

**Guardrails**: **none.** No guardrail gates custom memtable classes, the table `memtable` property, or `memtable_configurations` (`research-plugin-mechanics.md` §6). The only related validation is `TableParams.validate`: `cdc=true` is rejected iff the factory's `writesShouldSkipCommitLog()` is true (`TableParams.java:200-201`) — irrelevant here since the plugin keeps the default `false` (no CDC interaction, no double-durability).

**Silent-fallback hazard**: a node whose yaml lacks the config, or whose lib lacks the jar, logs an error and **silently runs SkipList** for the table — `MemtableParams.getWithFallback` (`schema/SchemaKeyspace.java:1070`). The node serves normally, just without exports. Two distinct failure paths: DDL on the coordinator fails outright via `TableAttributes.java:125` → `MemtableParams.get` throwing; but schema *arriving* at a node that can't instantiate falls back silently. CQLite must treat "no exports appearing for an opted-in table" as a **deployment fault**, not an empty memtable (§7 below).

---

### 7. Deployment/classpath constraints and 5.0-vs-trunk deltas

- **Deploy target is Cassandra 5.0** (verified on `origin/cassandra-5.0` @ `464b2e54`); CEP-11 (CASSANDRA-17034) shipped in **4.1**, so it is available on 5.0, not trunk-only (this corrects both an earlier draft of report-1 and the rough `sai-live-memtable-index.md` index, which wrongly says the pluggable API is "Trunk, since cassandra-6.0" — that claim is **superseded/wrong**; `Memtable.java:58`, `MemtableParams.java:111`, `performSnapshot` at `Memtable.java:426` all verified present on 5.0).
- **`git diff origin/cassandra-5.0..origin/trunk`** on the memtable package (~340 insertions), all source-breaking for a factory/memtable override (`research-plugin-mechanics.md` "Trunk deltas"):
  - `Memtable.Factory.createMemtableMetrics(TableMetadataRef)` → renamed `createMemtableMetricsReleaser(TableMetadataRef)` returning `Runnable` **[trunk]**.
  - `put` gains a 4th arg: `long put(update, indexer, opGroup, boolean assumeMissing)` becomes the abstract method **[trunk `Memtable.java:205`]**; 3-arg form becomes default.
  - New abstract members: `long getMemtableId()` **[trunk `:377`]**, `void notifyFlushed()` **[trunk `:429`]**, `ensureFlushListener(...)` **[trunk `:428`]** — a built-in flush-listener registry, "potentially *useful* to the exporter design later" — and `shouldSwitch(FlushReason, TableMetadata latest)` (1-arg form becomes default).
  - Snapshot path relocates from `ColumnFamilyStore.snapshot`'s inline memtable branch into `service/snapshot/TakeSnapshotTask.java` **[trunk `:136`]** — semantics unchanged, but 5.0 has no such class; cite CFS methods for 5.0.
  - `TrieMemtable` on trunk: still `public class ... extends AbstractShardedMemtable` **[trunk `:91`]**, still `factory(Map)` with only `shards` **[trunk `:756`]**, +130 internal lines. `MemtableParams.java`: **zero diff** — the reflection contract is stable across both.
  - `CellSourceIdentifier` exists on both 5.0 and trunk.
- **Split-package risk** (§2 above) is explicitly flagged as fragile against **future JPMS modularization**, not a present-day 5.0 blocker.
- **Per-major-version plugin builds required** for any trunk forward-port (5.0→trunk is not binary compatible) — productization deltas item 5 in `memtable-plugin-design.md` §9.
- **Repo placement** (NEEDS-DECISION (e)): proposed `integrations/cassandra-memtable-plugin/` — Java, own Gradle/Maven build, **excluded** from the cargo workspace and from `agent-gate.sh`'s Rust components.

---

### 8. The gc_grace orphaned-export correctness backstop

This is the **one** correctness exception in an otherwise-harmless tail/flush overlap design (`memtable-plugin-design.md` §6; `research-cqlite-tail-seam.md` §3). Cassandra's compaction purge (`maxPurgeableTimestamp`) considers only SSTables *it* knows about — **the tail dir is invisible to it**. If an export containing live row X (ts=100) is orphaned (plugin crash, never retired) and, ≥ `gc_grace` (default ~10 days) later, Cassandra compacts away both a covering tombstone (ts=200) and X, a merge of `{compacted gens + orphaned tail}` **resurrects X**.

For a properly-churned tail (retired on every flush via `discard()`, lifetime ≪ gc_grace), watermark dedup is purely an efficiency optimization — the backstop exists **specifically for the orphan case**.

**Chosen backstop for the spike**: max-age prune — the `CompositeSource` ignores tail exports whose manifest wallClock/mtime is older than a small TTL (minutes); ~40 LOC, no new parsing (`memtable-plugin-design.md` §6, NEEDS-DECISION (c)). **Precise deferred option**: promote the currently skip-parsed `commitLogIntervals` (`parser/repair_metadata.rs:621-633,740-747`, format known, version-gated at `version_gate/big.rs:22-26` / `bti.rs:18-19,64-65`) to a decode+expose, and drop any tail path whose interval is ⊇-covered by a flushed generation's interval — a pre-merge file-level prune exactly analogous to the existing token prune (`producer.rs:389-412`).

GC rules overall (`memtable-plugin-design.md` §4.5):
- *Superseded*: a new `gen-N` from the same memtable epoch supersedes `gen-(N-1)` — delete the older after publishing the newer.
- *Flushed*: on `discard()`, all of that memtable's exports are garbage — delete them; independently observable via a flushed sstable's `commitLogIntervals` covering the export's interval.
- *Crash-sweep*: on plugin startup, delete all `.staging-*` dirs.

---

### 9. OLTP-safety goals and prescribed benches

**G3 (OLTP safety)** from `memtable-plugin-design.md` §1: delegation-by-inheritance must preserve TrieMemtable's write path, `MemtablePool` accounting, cleaner eligibility, and flush signaling **exactly**; the exporter runs off the write hot path with *characterized* (not necessarily minimized) overhead.

Spike acceptance criteria mapped to concrete checks (`memtable-plugin-design.md` §8 table):
1. **Parity** — pre-flush merged read ≡ post-flush read on mixed upsert + row/range-tombstone workload, byte-equal under the parity comparator. In-crate `cqlite-flight/src/tail.rs` test module (new file — `producer.rs` is already 2,080 lines, over the campsite ratchet) plus an E2E Trino-driven stage exercising the on-demand handshake end-to-end.
2. **Write-path overhead** — JMH-or-equivalent bench: identical insert workload against `memtable='trie'` vs `memtable='cqlite'` (exporter idle, and under query-driven request load), report throughput + p99 deltas. Expected shape: near-zero on the write path itself (subclass adds no per-write code; on-demand means an idle table's exporter does nothing).
3. **Export-latency + amortization bench** (replaces a rejected cadence-curve bench): (1) end-to-end first-stale-read latency (marker → new `gen-*` visible → served) vs memtable size; (2) amortization across N queries in a burst; (3) handshake-only latency (WatchService delivery + dirty-check short-circuit) to decide whether the local-listener upgrade (§4.4(b)) is ever needed.
4. **Pool-accounting safety** — soak test: sustained write load sized to trigger pool-pressure flushes; assert normal-cadence flushes, no `MemtableCleaner` stalls, no OOM, monitor pinned-memory during overlapping export+flush.
5. **Spike report** — go/no-go, achieved on-demand freshness, stamped watermark description.
6. **(Added, highest-value de-risking test)** — `getFlushSet` on a never-switched-out memtable under concurrent writes is **untested upstream** (flush always calls it post-`switchOut`); dedicated stress test: continuous writes + concurrent repeated `getFlushSet`-iterate-serialize loops; assert no exception, no torn partition, exported content ⊇ everything written before each `P_start`.

Structural OLTP-safety guarantees baked into the design (not benches, but properties to hold): `getFlushSet` never calls `setFlushTransaction` so a concurrent real flush is unaffected; iteration doesn't block writes (lock-free trie reads vs single-writer-locked shards); exporter heap should be reported via `markExtraOnHeapUsed` if significant, and export buffers should stay bounded.

---

### 10. What LIVE per-query iteration would additionally require (vs. file export)

The design explicitly evaluated and **rejected for the spike** ("a2 — live surface"), deferring it as a measured follow-on only if export/handshake latency provably cannot meet an SLA (`memtable-plugin-design.md` §2 non-goals, §9 productization item; `report-1-memtable-freshness.md` §2(a) "a2"). What a2/live iteration would need beyond the file-export (a1) design:

- **A new in-JVM row-streaming surface** (local socket, shared memory, or in-JVM Flight) instead of writing to disk — CQLite's `MergeProducer` would need a **second, in-memory row seam**, because `SstableSource` is strictly **path-based**: `fn data_paths(&self) -> Result<Vec<PathBuf>>` (`cqlite-flight/src/producer.rs:80-83`) — the trait *cannot* accept an in-memory tail today; it must be materialized to a file, or the producer needs an entirely new second seam (`report-1` §1.1 "Correction"; `research-cqlite-tail-seam.md` §2 "If it were Arrow IPC instead ... an entirely new source+reconcile shim would be required").
- **True t+ε freshness at the cost of much higher effort/risk** — report-1 rates a1 = effort **M**, a2 = effort **L** ("JVM↔Rust bridge, new producer seam, op-order lifetimes"); report-2's posture (iii) "dual-write CEP-11 memtable" carries the same FFI crash-domain risk, now landing on the OLTP write path itself ("a bug in the analytics path can stall or abort OLTP writes").
- **An FFI crash-domain firewall.** A Rust panic crossing into the JVM aborts the whole process (`report-2` §2.2 table: "JNI/FFI bridge for iterator production + a crash-domain firewall (out-of-process or panic=unwind + catch) — new, no precedent" — "**no** — ASF will not take a native hot-path dependency in the storage core; fork/vendor-only"). CQLite's own `release-unwind` panic-unwind firewall (issue #1440) only softens this for CQLite's own bindings, not for a hypothetical in-JVM row-streaming path.
- **Op-order/lifetime management across the boundary** for the live iterator, correctness-equivalent to but *harder than* the readOrdering pin already required for file export (§3 above) — because a live per-query reader would hold the pin for the duration of the client's query, not a bounded background export.
- **Object-shape impedance if reusing Cassandra's own read path instead of a private tap** — `Memtable extends UnfilteredSource`, so any query-time iterator must produce full Cassandra `UnfilteredRowIterator`/`Row`/`Cell`/range-tombstone object graphs, meaning CQLite would materialize Java objects across FFI on every read, not just move bytes (`report-2` §2.1 "Memtable (CEP-11)").
- **No existing "flush completion"/read-hook hardware to build on** — the (superseded/rough) `memtable-api.md` and `memtable-pool-allocator-backpressure.md` indexes proposed hypothetical new interface seams (`Memtable.snapshotIterator(long maxTimestamp, ColumnFilter)`, `ColumnFamilyStore.forceFlushForRead()`, `Memtable.Owner.onFlushCompleted(...)`) that **do not exist today** — building live per-query access via any of these would be new upstream API surface, not a plugin.
- **SAI's read-time index-merge precedent is a partial analogy, not a template** — `IndexSearchResultIterator` (`index/sai/disk/IndexSearchResultIterator.java:42-134`) already unions live-memtable-index postings with on-disk-index postings at query time with no flush required (`sai-live-memtable-index.md` "Read-Time Merge Architecture"), proving the *general pattern* of live-memtable read access is upstream-supported for secondary indexes — but it operates on term postings (`PrimaryKey` sets), not row data, is invoked only when a secondary index exists (DDL-gated, not automatic on all tables), and still must wrap results as `UnfilteredPartitionIterator` (conversion-layer cost, `sai-live-memtable-index.md` "Hard Couplings" #4) — it does not eliminate the FFI/object-shape work above for a full-row analytical tap.
- **Counters remain excluded regardless of trigger mechanism** — pure LWW cannot reconcile counter shards (Cassandra context-merges them); this constraint is orthogonal to file-export vs live-iteration and applies to both.

**Bottom line held by both reports and the design doc**: file-export (a1/posture i-ii-hybrid) reuses zero new merge code and inherits CQLite's existing byte-parity reconcile untouched; live iteration (a2/posture iii) would require a new producer seam, an FFI crash-domain firewall Cassandra's own maintainers are described as unwilling to accept in the storage core, and materially higher effort (L vs M) for a freshness gain (true t+ε vs sub-second export cadence) that the owner explicitly did not require for the spike.

### 5.x Additional requirements introduced by this exploration (beyond the #1807 corpus)

1. **Live-stream sink (v2):** a per-query iteration entry point (query-scoped `readOrdering` pin; chunked emission; pin-duration budget with snapshot fallback), streaming the flush row serialization over UDS/shared memory instead of `SSTableTxnWriter`-to-disk. Same iteration machinery, different sink.
2. **Generation pinning for live-set scans (WS3):** CQLite scanning the live data dir races compaction deleting files mid-scan. Candidates: in-JVM tracker refs exposed via a plugin pin hook; hardlink snapshots per scan; tail-dir-style publication of a pinned manifest. (#1406 snapshot-hygiene territory.)
3. **Coordinator surface (WS5):** whatever the OLAP verb is (CQL grammar vs node-local Flight endpoint + internal fan-out), the plugin side must expose per-table tail access control and the freshness knob (sync-export / live-stream / bounded-staleness) per request.
4. **Process boundary (WS4):** in-JVM JNI (leans on the #1440 panic-unwind firewall posture) vs colocated companion process fronted by the coordinator. Note: a companion process fronted by the coordinator still satisfies "not a separate path" — separateness is about who you send queries to, not process count.

---

## 6. The per-generation Arrow cache (cache-not-copy)

The scan engine may keep **per-generation, pre-reconciliation Arrow segments** so repeated scans skip `nb` decode:

- **Keyed to SSTable generation; invalidated exactly at generation death** (compaction). The engine sees the live set change (compaction-manager #905 planner territory). Rebuild is lazy and proportional to compaction throughput — the same bytes compaction already rewrote.
- **A cache, never a correctness artifact.** Cold cache = slower scan, never a wrong answer. No dual-format compaction; nothing to keep transactionally consistent with `nb`.
- **Segments are pre-merge and carry the LWW envelope** (per-column writetime/TTL columns, row/partition deletion columns, separate range-tombstone batch, exploded multi-cell collections with per-element metadata). This is the one place the envelope earns its complexity: it lets the merge run columnar over cached generations while row-merging the (tiny) tail.
- Hot/cold tiering is an optimization decision inside the cache, invisible to correctness — the full on-disk format survey and recommendation ladder (Parquet + zone-map sidecar today; Vortex as the benched escalation candidate; boring Parquet/Iceberg for any shared cold tier) is §11.

This directly answers the "Parquet would be constantly rewritten" objection: eviction = generation death; compaction does one job; the columnar copy is disposable.

---

## 7. Prior art in the Cassandra ecosystem

### (a) `cassandra-analytics` / Spark Bulk Reader + Sidecar

**Mechanics.** The Apache Cassandra Sidecar — a separate JVM process per node, decoupled from the Cassandra release cycle — exposes analytics-specific HTTP endpoints: snapshot create/list/clear, `stream-sstable` (raw SSTable component bytes), `schema`, `replica ranges` (token ownership including pending ranges), and keyspace/ring/cluster-version info. The Spark Bulk Reader (SBR, part of `apache/cassandra-analytics`, CEP-28) has the Sidecar **create a Cassandra snapshot** (hardlinks to the currently flushed SSTables) on each replica, then streams the raw SSTable bytes for the relevant token range over HTTP into Spark executor JVMs.

**Where reconciliation happens.** Merge/reconciliation happens **client-side, inside the Spark executor**, and it is not a from-scratch re-implementation: SBR reuses Cassandra's own `CompactionIterator` (wrapped by `SparkRowIterator`/`SparkCellIterator`) to perform what the project calls **"streaming compaction"** — the same tombstone-correct merge semantics as a real major compaction, except the output goes to CQL rows / a Spark DataFrame instead of a new SSTable on disk. `PartitionedDataLayer` reads from enough replicas to satisfy a caller-specified consistency level (`LOCAL_QUORUM`, etc.); `EACH_QUORUM` was still unimplemented as of the CEP-era docs reachable in this search *(unverified currency — may have shipped since)*.

**Freshness.** Strictly **snapshot/flushed-SSTable-only**. The unflushed memtable is explicitly excluded — a documented staleness window, not an incidental gap. The CEP text itself frames the cost as "network traffic and the cost of snapshots," i.e., it accepts point-in-time staleness as the design, not a bug. Snapshots are created per job and cleared "on a best-effort basis" after completion; TTL support for snapshots was only added later (CASSANDRA-19273 / CASSANDRA-16451) — leaked/orphaned snapshots pinning disk space was a real enough operational problem to need a dedicated fix. Vnodes are explicitly called out as untested/unsupported in the CEP write-up *(unverified currency)*. Tombstone handling is not separately documented beyond "`CompactionIterator` does it" — correctness is inherited from compaction semantics, but the CEP text has no explicit tombstone-GC/`gc_grace` discussion for the analytics path.

**Weakness (confirmed).** This is fundamentally a **second compute tier** — a Spark cluster with its own lifecycle, security perimeter, resource footprint, and job-scheduling story, wired to the serving cluster only through the Sidecar's HTTP surface.

**Historical predecessor — DSE Analytics.** DataStax DSE Analytics ran Spark master/workers **co-located inside a dedicated "Analytics" datacenter** (HA coordination via a `dse_analytics` system table). DSE went so far as to offer **"Analytics Solo" datacenters** devoted entirely to analytics, separated from OLTP datacenters — a tacit admission that colocating analytics compute with transactional replicas causes contention, with topological separation as the escape valve rather than in-process resource sharing. Underneath, the open-source **DataStax Spark Cassandra Connector** — the actual read mechanism DSE Analytics relied on — reads via **CQL token-range queries with predicate pushdown**, one Spark task per Cassandra partition/split. This is *not* a raw-SSTable bulk path; it inherits full CQL coordinator/read-path cost per row (serialization, per-query reconciliation), architecturally older/slower than the newer `cassandra-analytics` SSTable-streaming model. Lesson: even DataStax's most integrated historical offering never bypassed the coordinator read path for bulk analytics — it "just" fanned out ordinary CQL queries across a co-located Spark deployment.

**A real production data point — Netflix.** Netflix's **Aegisthus** (~2012, now archived/unmaintained, no support for later Cassandra versions) ran a MapReduce job over raw SSTables (from Priam-archived backups or full snapshots), reducing per-key to one consistent view and emitting JSON to S3 — the same "materialize once, external merge" shape as `cassandra-analytics`, a decade earlier. Netflix's *current* "Cassandra Analytics Wrapper" is built on the open-source `cassandra-analytics`, but sources data from **Netflix's own S3 backup representation**, not live Sidecar streaming — even Netflix's modern path is backup/point-in-time-oriented, producing standard Spark DataFrames for downstream connectors, not a live in-cluster query. Takeaway: the most mature, longest-running production user of Cassandra bulk analytics still treats it as an offline batch materialization off backups *(unverified — Netflix's internal docs are not public; inferred from the public tech-blog post only)*.

Sources: [CEP-28 wiki](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-28:+Reading+and+Writing+Cassandra+Data+with+Spark+Bulk+Analytics) · [apache/cassandra-analytics](https://github.com/apache/cassandra-analytics) · [cassandra-analytics README](https://github.com/apache/cassandra-analytics/blob/trunk/README.md) · [CASSANDRA-16222](https://issues.apache.org/jira/browse/CASSANDRA-16222) · [jberragan/spark-cassandra-bulkreader README](https://github.com/jberragan/spark-cassandra-bulkreader/blob/develop/README.md?plain=1) · [CASSANDRA-9259](https://issues.apache.org/jira/browse/CASSANDRA-9259) · [CASSANDRA-19273 (mail-archive)](https://www.mail-archive.com/commits@cassandra.apache.org/msg296420.html) · [CASSANDRA-16451](https://issues.apache.org/jira/browse/CASSANDRA-16451) · [DSE Analytics component architecture](https://docs.datastax.com/en/dse/6.9/architecture/component-architecture/analytics.html) · [DSE 5.1 architecture FAQ](https://docs.datastax.com/en/dse/5.1/architecture/database-architecture-faq.html) · [datastax/spark-cassandra-connector README](https://github.com/datastax/spark-cassandra-connector/blob/master/README.md) · [The Evolution of Cassandra Data Movement at Netflix](https://netflixtechblog.com/the-evolution-of-cassandra-data-movement-at-netflix-6e13329c80a1) · [Netflix/aegisthus (archived)](https://github.com/Netflix/aegisthus) · [Aegisthus — A Bulk Data Pipeline out of Cassandra](https://medium.com/netflix-techblog/aegisthus-a-bulk-data-pipeline-out-of-cassandra-984882557fa)

---

### (b) SAI's per-SSTable attachment lifecycle — and why it does not transfer to scannable Parquet

**Build at flush.** CEP-7 Storage-Attached Indexes are literally attached to their SSTable — built alongside it and destroyed with it; there is no separate "index generation" lifecycle independent of SSTable lifecycle. When a memtable flushes, SAI reuses the *index memtable* (an in-memory structure already tracking indexed values) to generate the on-disk index "to avoid re-indexing the flushed sstable twice." Flush proceeds in two phases: (1) row-ID generation — on-disk row-ID→token mappings, SSTable partition offsets, and a temporary primary-key↔row-ID mapping are written; (2) per-column indexing — SAI iterates the memtable index to emit token-sorted `(term, row-ID)` pairs into segment files.

**Build at compaction.** Rather than naively re-index the merged output, SAI buffers indexed values + row IDs *in token order* as compaction produces the new SSTable, flushing accumulated segment buffers to disk to bound heap use. Each index group installs an SSTable Flush Observer that coordinates writing all attached column indexes alongside the new SSTable writer. Components are organized as **segments** (smallest on-disk unit, multiple segments per physical file) plus a **primary-key store** written once per SSTable and shared across all column indexes on that table (bidirectional row-ID↔primary-key lookup), avoiding duplication. A marker file flags successful completion so missing-vs-empty index states are distinguishable.

**Drop with SSTable.** Index components are co-located with the SSTable and removed automatically when it's compacted away/deleted, driven by the standard "SSTable List Changed Notification" that names SSTables added and removed in a compaction transaction. *(Unverified in detail: exact `SSTableFlushObserver`/notification-handler source could not be pulled from the CASSANDRA-16052 Jira text itself — resolved/fixed in 5.0-alpha1/5.0, but confirming the code path would need a direct source read.)*

**Why queries still route through Cassandra's reconciling read path.** This is the load-bearing design fact. **SAI does not index tombstones.** The index only narrows candidates — for a single predicate it unions matching row-IDs per-SSTable-index in token order; for multiple predicates it intersects those sets. The index hands back *partition keys / row-IDs*, and Cassandra does a normal partition read across memtable + all relevant SSTables, applying "a filter tree" for tombstones, partition-level granularity, and any non-indexed expressions. In other words: **SAI is a pointer index, not an answer cache** — the actual liveness/shadowing decision is deferred entirely to the standard multi-source merge read, exactly as if no index existed.

**Why this lifecycle does NOT transfer to a scannable per-SSTable Parquet attachment.** SAI can afford "attach index to SSTable, drop with SSTable" *because the index is never asked to answer the query by itself* — it only prunes, and the row-level, tombstone-aware, cross-generation merge always happens afterward through the existing read path. A **scannable** Parquet attachment inverts this: it *is* meant to be read as (part of) the answer, not merely a pointer back to raw rows. That breaks the SAI trick two ways:

- **Cross-SSTable tombstone shadowing.** A value present in one generation's Parquet attachment may be shadowed by a tombstone or a newer write living in a *different* SSTable generation, or still in the (unflushed) memtable. A single per-SSTable Parquet file has no way to know that at scan time — it's not wrong about its own SSTable's contents, but it's wrong (or dangerously incomplete) as a standalone answer.
- **Who does the merge.** SAI defers the merge to the existing row-level reconciling read path, which is expensive per-row but correct. A per-SSTable Parquet attachment used *as* the scan result would need something else — either the coordinator/reader re-implementing full LWW+tombstone reconciliation across attachments (essentially re-deriving compaction-time semantics at query time, on every query, against a moving set of generations), or accepting staleness/incorrectness.

This is precisely the gap the CQLite node-local merge-then-emit design fills — it explicitly keeps the columnar cache *not* independently query-answerable; CQLite performs the k-way LWW merge across the tail + flushed generations first, and only emits Arrow *post-merge*, invalidating any cache at compaction rather than trying to make per-generation columnar segments self-sufficient the way SAI's per-SSTable pointer index gets to be.

Sources: [CEP-7 wiki](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-7%3A+Storage+Attached+Index) · [SAI concepts — Cassandra docs](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/indexing/sai/sai-concepts.html) · [CASSANDRA-16052](https://issues.apache.org/jira/browse/CASSANDRA-16052)

---

### (c) CEP-11 pluggable memtables — API surface summary

**Motivation.** Escape the GC tradeoff between long pauses and small memtables; enable off-heap/alternate-format memtable structures (including, e.g., persistent-memory-backed implementations).

**Verbatim interface surface** (from `Memtable.java` / `Memtable_API.md` on trunk):

```java
// Memtable core
long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup);
long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, boolean assumeMissing);
FlushablePartitionSet<?> getFlushSet(PartitionPosition from, PartitionPosition to);
void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound);
void discard();
boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition);

// Memtable.Factory
Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                 TableMetadataRef metadataRef, Owner owner);
default boolean writesShouldSkipCommitLog();
default boolean writesAreDurable();
default boolean streamToMemtable();
default boolean streamFromMemtable();

// Memtable.Owner (implemented by ColumnFamilyStore)
Future<CommitLogPosition> signalFlushRequired(Memtable memtable, ColumnFamilyStore.FlushReason reason);
Memtable getCurrentMemtable();
Iterable<Memtable> getIndexMemtables();
ShardBoundaries localRangeSplits(int shardCount);
```

**Lifecycle.** `put()` during normal operation → `getFlushSet(from, to)` extracts a flushable partition range when a flush is triggered → `switchOut(writeBarrier, commitLogUpperBound)` seals the memtable against further writes and records the commit-log position it covers → `discard()` releases all held memory once the flush (or a discard, e.g. on truncate) is durable. `Owner` is the `ColumnFamilyStore`-side contract back to whatever holds the memtable (request a flush, fetch the current/index memtables, get shard boundaries for range-sharded implementations). "Allocator" concepts live in the support classes, not the interface itself: `AbstractAllocatorMemtable` provides shared off-heap/on-heap `Allocator`-based memory accounting used by `TrieMemtable` and `ShardedSkipListMemtable`.

**Built-in implementations.** `SkipListMemtable` (default), `ShardedSkipListMemtable` (token-space-sharded skiplists, configurable shard count), `TrieMemtable` (places the partition-indexing trie in a buffer, optionally off-heap, for GC efficiency — landed via CEP-19/CASSANDRA-17240, the trie-memtable paper).

**Configuration.** Cluster-wide default via `cassandra.yaml`:
```yaml
memtable:
  class: SkipListMemtable
```
Named, reusable configurations (`memtable_templates`, depending on version) bundle a class + params:
```yaml
memtable_templates:
  trie:
    class: TrieMemtable
    shards: 16
```
Per-table override via DDL, referencing either a configuration name or an inline class+params map:
```sql
CREATE TABLE ... WITH memtable = { 'class' : 'HashOrderedMapMemtable' };
```
Selection is validated only at instantiation time for a given table; nodes lacking a referenced configuration fall back to the default to avoid schema disagreement across a mixed-version cluster.

**Relevance to the ArrowMemtable proposal.** CEP-11 already explicitly contemplates memtables that hold data in "alternate formats" and hand data off externally rather than flushing through the standard path — the design doc calls out persistent-memory-backed memtables that "never flush" and instead spill/migrate by other means. That's the same shape of extension point an `ArrowMemtable extends TrieMemtable` would use: reuse `TrieMemtable`'s data structure and `AbstractAllocatorMemtable` memory accounting, but override `getFlushSet`/expose the unflushed tail through a different channel (export as a real `nb` SSTable snapshot, or stream in native row serialization) rather than only flushing through the conventional writer.

Sources: [CEP-11 wiki](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-11:+Pluggable+memtable+implementations) · [Memtable_API.md, trunk](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/memtable/Memtable_API.md) · [Memtable.java, trunk (raw)](https://raw.githubusercontent.com/apache/cassandra/trunk/src/java/org/apache/cassandra/db/memtable/Memtable.java) · [Pluggable Memtable Implementations — Cassandra 4.1 blog](https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-Features-Pluggable-Memtable-Implementations.html) · [CEP-19 wiki](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-19:+Trie+memtable+implementation) · [VLDB trie-memtables paper](https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf)

---

### (d) Columnar/Parquet proposals in Cassandra — none shipped

Nothing concrete has shipped, or gone to a CEP vote, for a Parquet-backed or general columnar SSTable format inside Cassandra itself. Two old, still-open tickets are the closest precedent, plus one shipped CEP that explicitly *avoided* introducing a new format:

- **[CASSANDRA-7447](https://issues.apache.org/jira/browse/CASSANDRA-7447) — "New sstable format with support for columnar layout"** (filed 2014). Proposes three specialized storage primitives: a partition-key layer with O(1) lookup via a split-ordered list, a cache-oblivious trie for clustering/primary keys enabling efficient multi-SSTable merges and early range-tombstone handling, and a columnar values layer with delta/offset encoding + configurable field grouping to trade off row- vs column-oriented access. **Status: still Open/Unresolved**, fix-version pinned to a placeholder "6.x", last touched March 2026 per Jira metadata. It supersedes an even older ticket, CASSANDRA-6810. No CEP was ever derived from it, and the trie-oriented ideas it floated were separately realized (in narrower form) by CEP-19 (trie memtables) and CEP-25 (trie-indexed BTI SSTable format) — but the columnar-values-layer part of the proposal was never picked up. This reads as stalled from scope: a full row+column hybrid format touching read/write/compaction end-to-end, with no active champion.

- **[CASSANDRA-9259](https://issues.apache.org/jira/browse/CASSANDRA-9259) — "Bulk Reading from Cassandra"** (filed 2015). This is the one place a community ticket names Parquet explicitly: *"For example, we might employ Parquet (just as an example) to store the same data as in the primary Cassandra storage (aka SSTables)."* The proposal frames bulk reads as "streaming compaction" — run a read like a major compaction, but emit CQL rows to a stream instead of a new SSTable — and floats an *alternate storage format* (Parquet named only as an illustrative example, not committed to) as one option for serving that stream faster. **Status: still Open/Unresolved**, target "6.x". Only narrower sub-tasks landed (CASSANDRA-11520 optimized local read path, CASSANDRA-11521 streaming for bulk reads, CASSANDRA-11542 HDFS benchmark comparison) — the Parquet-as-alternate-format idea itself was never pursued past the mention.

- **[CEP-28: Reading and Writing Cassandra Data with Spark Bulk Analytics](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-28:+Reading+and+Writing+Cassandra+Data+with+Spark+Bulk+Analytics)** — the CEP that actually shipped (Accepted, released in 5.0.0, 2023) in this space. Notably it **deliberately does not** introduce any columnar/Parquet format: it works directly with standard SSTables, uses `CQLSSTableWriter` to produce ordinary version-appropriate SSTables for bulk writes, and reuses Cassandra's own classes as a library for local processing on the read side. It gives essentially no attention to tombstone/merge semantics beyond "follows standard Cassandra practice" — it sidesteps the exact cross-generation-merge problem this research is about, rather than solving it.

**External tooling that materializes to Parquet (outside Cassandra proper, not a CEP/JIRA proposal).** Two independent, unrelated implementers converge on the identical "snapshot → columnar materialize → separate external engine" pattern, confirming it as the industry default rather than a CEP-level design:
- Philip Moore's (Voltron Data) Cassandra Summit talk **"OLAP on Your Cassandra Data with Arrow, Flight SQL, ADBC, and DuckDB"**: convert Sidecar-sourced SSTable snapshots to Parquet, then serve via an external Arrow Flight SQL server backed by DuckDB, with ADBC avoiding intermediate conversions on the query side. Not coordinator-embedded.
- **Instaclustr's `sstable-transformer`**: Sidecar-streamed SSTables → Parquet/Avro (optionally into ClickHouse), reusing `cassandra-analytics`'s `SparkRowIterator`/`CompactionIterator` for the merge (`ONE_FILE_ALL_SSTABLES` mode reconciles tombstones/duplicates the same way; `ONE_FILE_PER_SSTABLE` explicitly does not). Its README states plainly: **no incremental processing** — a full re-transformation is required to pick up new data.

**Bottom line.** No CEP, no accepted design, and no active JIRA effort proposes attaching Parquet (or any columnar format) to SSTables as a queryable artifact. The two tickets that even raise the idea (CASSANDRA-7447, CASSANDRA-9259) are a decade old, still unresolved, and Parquet is mentioned in only one of them as an illustrative example, not a committed design. The one CEP that did ship in the "bulk/analytics" lane (CEP-28) explicitly chose to stay inside the existing SSTable format rather than adopt anything columnar. This leaves CQLite's node-local, merge-then-cache-as-Arrow approach without close CEP-level prior art to align with or diverge from explicitly — a novel position relative to the project's own history, not an extension of a stalled proposal.

Sources: [CASSANDRA-7447](https://issues.apache.org/jira/browse/CASSANDRA-7447) · [CASSANDRA-9259](https://issues.apache.org/jira/browse/CASSANDRA-9259) · [CEP-28 wiki](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-28:+Reading+and+Writing+Cassandra+Data+with+Spark+Bulk+Analytics) · [instaclustr/sstable-transformer](https://github.com/instaclustr/sstable-transformer) · [OLAP on Your Cassandra Data with Arrow, Flight SQL, ADBC, and DuckDB — Philip Moore, Voltron Data (YouTube)](https://www.youtube.com/watch?v=HtCNe_B7okk) · [Trino Cassandra connector docs](https://trino.io/docs/current/connector/cassandra.html)

---

### (e) Why coordinator-native is unclaimed territory

A direct search of the CEP index, Cassandra dev mailing-list archives, and JIRA for a **coordinator-native or in-node OLAP/analytics-engine proposal found none that shipped or even reached a DISCUSS/VOTE thread**. The closest adjacent CEPs — [CEP-40: Data Transfer Using Cassandra Sidecar for Live Migrating Instances](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-40:+Data+Transfer+Using+Cassandra+Sidecar+for+Live+Migrating+Instances) and [CEP-45: Bulk transfer](https://www.mail-archive.com/commits@cassandra.apache.org/msg327487.html) — are about instance replacement/streaming, not analytics query. **This is a negative result** — treat it as "no such proposal found in this search," not proof none exists.

Every real implementation surveyed above converges on the same shape instead — **materialize once (snapshot or backup), merge externally, serve from a second compute tier**:

| System | Merge location | Freshness | Compute tier |
|---|---|---|---|
| Spark Bulk Reader + Sidecar (CEP-28) | Spark executor (`CompactionIterator` reused) | Flushed-SSTable-only, snapshot-based | Separate Spark cluster |
| DSE Analytics | Coordinator read path per CQL query | Live, but full per-row CQL cost | Co-located Spark DC ("Analytics Solo") |
| Netflix Aegisthus / Analytics Wrapper | MapReduce/Spark reduce over backups | Point-in-time (S3 backup), not live | Offline batch off backups |
| Voltron Data (Arrow/Flight SQL/DuckDB) | External DuckDB over converted Parquet | Snapshot-based | External Arrow Flight SQL server |
| Instaclustr `sstable-transformer` | `CompactionIterator` reused, no incremental mode | Full re-transform required for new data | External tool/ClickHouse |
| SAI (CEP-7) | Deferred to the standard reconciling read path | Live (it's not a cache, just a pointer index) | In-process, but never answers alone |

No system in this survey performs the tombstone-aware cross-generation merge **in-process, node-local, against live (including unflushed) state, and emits a directly query-answerable columnar result**. SAI comes closest structurally (per-SSTable attachment, in-process) but deliberately never lets the index answer a query by itself — see (b). Everything analytics-shaped (a) and (d) instead pushes the merge to a second compute tier or accepts snapshot-era staleness, and no CEP/JIRA proposal (e) has ever attempted the coordinator-native version. This is the gap the ArrowMemtable + node-local CQLite tombstone-aware scan engine design targets — not a rejected or abandoned idea, but one nobody in this ecosystem's public history has proposed, let alone shipped.

**How the CQLite coordinator-native design removes each recurring weakness:**

| Prior-art weakness (verified across ≥2 independent implementations) | How the coordinator-native design removes it |
|---|---|
| **Separate compute tier**: Spark cluster / DSE Analytics Solo DC, external DuckDB/ClickHouse — a wholly distinct operational path from serving. | Node-local: CQLite runs as a library alongside the memtable plugin — no second cluster, no second security perimeter, no separate job scheduler. |
| **Snapshot-only freshness**: unflushed memtable content is explicitly excluded from every system surveyed (SBR, sstable-transformer, Aegisthus, Netflix's wrapper). | The unflushed tail is handed to CQLite directly (exported `nb` snapshot or live native-row stream), merged with flushed SSTables in the same k-way LWW pass — no analytics-side staleness window beyond normal flush cadence. |
| **Snapshot lifecycle fragility**: best-effort cleanup, TTL bolted on later (CASSANDRA-19273) after leaked snapshots consumed disk in the field. | No operator-managed Cassandra snapshot directory to leak — export/stream is per-query and ephemeral, or a cache invalidated at compaction, not a standing hardlink tree. |
| **Duplicated merge implementation across runtimes**: SBR/sstable-transformer re-host Cassandra's `CompactionIterator` inside a *second* JVM process (Spark) to redo the same merge Cassandra already does — two codepaths that can drift, with disclosed gaps (`EACH_QUORUM` unimplemented, vnodes untested at CEP-28 time). | One merge implementation, node-local, operating on the same generation of on-disk/tail data the coordinator itself would read — no second JVM, no divergent reconciliation logic to keep in sync. |
| **Batch/materialize-then-query with no incremental story** (Instaclustr's own README: "no incremental processing... requires full SSTable re-transformation for updates"; same shape in Voltron/DuckDB and Aegisthus). | Arrow batches are produced post-merge per query against current state; the columnar cache (if used) is invalidated at compaction rather than requiring a full external batch re-run to reflect new writes. |
| **CQL-token-range read cost** (DSE-era Spark connector): per-row CQL serialization/coordinator cost, no bulk columnar path at all. | Node-local merge emits Arrow batches directly — no per-row CQL round trip, no coordinator serialization tax. |
| **Per-SSTable index/attachment can't stand alone as an answer** (SAI): cross-generation tombstone shadowing means a pointer index must defer to the full reconciling read path. | CQLite performs the k-way LWW merge across tail + flushed generations *before* emitting Arrow — the columnar output is never asked to be a standalone per-generation answer. |
| **Even the most mature production deployment (Netflix) still reads from S3 backups, not live cluster state** — reinforcing that nobody has shipped a live in-cluster path. | This is the gap the coordinator-native design explicitly targets; no negative prior art was found that contradicts feasibility, only that nobody has shipped it. |

**Caveats / unverified claims carried through this section**: CEP-28's `EACH_QUORUM`/vnode limitations may have been resolved since the cited text was written; the "no coordinator-native proposal ever shipped" conclusion is a negative result from a non-exhaustive search of CEP/JIRA/mailing-list archives, not a certified absence; Netflix's internal architecture details beyond the public tech-blog post are unverified; the exact `SSTableFlushObserver`/notification-handler mechanics for SAI component removal on compaction were not confirmed from primary source (Jira ticket text only).

#### Combined source list
- [CEP-28: Reading and Writing Cassandra Data with Spark Bulk Analytics](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-28:+Reading+and+Writing+Cassandra+Data+with+Spark+Bulk+Analytics)
- [apache/cassandra-analytics (GitHub)](https://github.com/apache/cassandra-analytics)
- [cassandra-analytics README](https://github.com/apache/cassandra-analytics/blob/trunk/README.md)
- [CASSANDRA-16222 (JIRA, CEP-28 tracking)](https://issues.apache.org/jira/browse/CASSANDRA-16222)
- [jberragan/spark-cassandra-bulkreader README](https://github.com/jberragan/spark-cassandra-bulkreader/blob/develop/README.md?plain=1)
- [CASSANDRA-9259: Bulk Reading from Cassandra (JIRA)](https://issues.apache.org/jira/browse/CASSANDRA-9259)
- [CASSANDRA-19273 / snapshot TTL for analytics bulk reader (mail-archive)](https://www.mail-archive.com/commits@cassandra.apache.org/msg296420.html)
- [CASSANDRA-16451: Add ability to TTL snapshots](https://issues.apache.org/jira/browse/CASSANDRA-16451)
- [DSE Analytics component architecture (DataStax docs)](https://docs.datastax.com/en/dse/6.9/architecture/component-architecture/analytics.html)
- [DSE 5.1 architecture FAQ](https://docs.datastax.com/en/dse/5.1/architecture/database-architecture-faq.html)
- [datastax/spark-cassandra-connector README](https://github.com/datastax/spark-cassandra-connector/blob/master/README.md)
- [The Evolution of Cassandra Data Movement at Netflix (Netflix Tech Blog)](https://netflixtechblog.com/the-evolution-of-cassandra-data-movement-at-netflix-6e13329c80a1)
- [Netflix/aegisthus (GitHub, archived)](https://github.com/Netflix/aegisthus)
- [Aegisthus — A Bulk Data Pipeline out of Cassandra (Netflix Tech Blog)](https://medium.com/netflix-techblog/aegisthus-a-bulk-data-pipeline-out-of-cassandra-984882557fa)
- [instaclustr/sstable-transformer (GitHub)](https://github.com/instaclustr/sstable-transformer)
- [Trino Cassandra connector docs](https://trino.io/docs/current/connector/cassandra.html)
- [OLAP on Your Cassandra Data with Arrow, Flight SQL, ADBC, and DuckDB — Philip Moore, Voltron Data (YouTube)](https://www.youtube.com/watch?v=HtCNe_B7okk)
- [CEP-40: Data Transfer Using Cassandra Sidecar for Live Migrating Instances](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-40:+Data+Transfer+Using+Cassandra+Sidecar+for+Live+Migrating+Instances)
- [CASSANDRA-20383 / CEP-45: Bulk transfer (mail-archive)](https://www.mail-archive.com/commits@cassandra.apache.org/msg327487.html)
- [CEP-7: Storage Attached Index (wiki)](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-7%3A+Storage+Attached+Index)
- [Storage-attached indexing (SAI) concepts — Cassandra docs](https://cassandra.apache.org/doc/latest/cassandra/developing/cql/indexing/sai/sai-concepts.html)
- [CASSANDRA-16052 — CEP-7 Storage Attached Indexes (Phase 1), Jira](https://issues.apache.org/jira/browse/CASSANDRA-16052)
- [CEP-11: Pluggable memtable implementations (wiki)](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-11:+Pluggable+memtable+implementations)
- [Memtable_API.md, apache/cassandra trunk (GitHub)](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/memtable/Memtable_API.md)
- [Memtable.java, apache/cassandra trunk (GitHub, raw)](https://raw.githubusercontent.com/apache/cassandra/trunk/src/java/org/apache/cassandra/db/memtable/Memtable.java)
- [Pluggable Memtable Implementations — Cassandra 4.1 blog](https://cassandra.apache.org/_/blog/Apache-Cassandra-4.1-Features-Pluggable-Memtable-Implementations.html)
- [CEP-19: Trie memtable implementation (wiki)](https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-19:+Trie+memtable+implementation)
- [Trie Memtables in Cassandra, VLDB 2022 paper](https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf)
- [CASSANDRA-7447 — New sstable format with support for columnar layout, Jira](https://issues.apache.org/jira/browse/CASSANDRA-7447)

---

## 8. Ways to keep this as simple as possible

1. **Zero new storage formats, zero new serializers.** Tail = Cassandra's own serialization; output Arrow = post-merge (no envelope); the only envelope lives in an optional cache. The entire novel surface is: the streaming channel, generation pinning, and the coordinator verb.
2. **Reuse the three hard things we already built and parity-test:** the `nb` decoder, the k-way LWW merge, and token-range fan-out (Trino connector + #1336).
3. **Ship the freshness ladder bottom-up.** v1 snapshot is the approved, de-risked #1807 design — file, atomic rename, watermark, `sstabledump`-debuggable. v2 live-stream is the same iteration with a different sink. Never build v2's protocol before v1 proves the merge + fan-out.
4. **Cache is optional at every stage.** The path is correct with no cache at all (scan `nb` per query); the cache is a pure performance layer added when WS7's numbers say so.
5. **Companion process before JNI** unless WS4 proves otherwise — process isolation keeps a Rust panic/OOM from taking down the coordinator JVM, and UDS/shared-memory Arrow is cheap on-host.
6. **Consistency contract stated, not engineered:** per-token-range CL.ONE snapshot semantics (same as every Cassandra analytics approach ever shipped). Quorum-merged OLAP scans are possible in principle and explicitly out of scope.
7. **Don't re-litigate settled ground:** extend-not-wrap (instanceof gates, verified against source), no CDC (owner), no pre-`na` formats, `nb` stays load-bearing.

---

## 9. Related work map (this repo)

### #1807: CEP-11 memtable plugin spike
**State:** OPEN · **Title:** Spike: CEP-11 'cqlite' memtable plugin — export unflushed tail for Flight/Trino freshness (no CDC)

Spike proves CEP-11 custom Memtable implementation that wraps TrieMemtable (delegate all behavior), exports live partitions to Arrow IPC / temp-SSTable on flush with watermark, and merges unflushed tail with flushed SSTables for freshness without CDC complexity. Acceptance criteria: parity under mixed upsert + tombstone workload, write-path overhead measurement, MemtablePool integrity. **Status:** Ready for flow-activate after spike (OpenSpec design-driven).

**Relevance:** Directly implements the ArrowMemtable export mechanism for the coordinator-native OLAP path; critical infrastructure for in-cluster snapshot freshness without fork/CDC burden.

---

### #1934: CQLite two-project split exploration
**State:** OPEN · **Title:** Exploration epic: CQLite two-project split — SQLite-like surface vs Cassandra storage/analytics engine

Exploration epic (decision packet, not code moves) on whether to split CQLite into surface layer (CLI, Python/Node/WASM bindings) and separately-named core engine for Cassandra data + connectors (Trino, Flight, DataFusion, Iceberg, memtable). Research identified clean lines (bindings use ~4-5 facade imports, Trino template already separate, storage has zero reverse deps, feature flags partition nearly along boundary) and gaps (query engine straddles, flight consumes internals, shared vocabulary). **Status:** Council review completed; decision packet ready (owner-gated).

**Relevance:** Architectural foundation; engine tier needs clear API contracts and boundaries for robust CEP-11 plugin embedding and multi-JVM/process deployment scenarios.

---

### #941: DataFusion table provider epic (Design A)
**State:** OPEN · **Title:** [EPIC] DataFusion table provider — Design A: co-located Flight-backed provider (Trino stays MPP owner)

Epic (scaffolded 2026-07-04) to build Flight-backed DataFusion TableProvider over Sidecar snapshot manifests; Trino remains MPP scheduler, DataFusion is leaf scan surface only. Design A is approved first (co-located Flight), Design B (remote Sidecar HTTP-Range) secondary, Design C (Iceberg materialized epoch) future. Eight non-negotiable invariants bind all children (snapshot ≠ consistent, Inexact default, real cancellation, PlanProperties/stats, byte_cap governance, wraparound membership, hard affinity, signed versioned manifest). **Status:** All 8 children (A1–A8) at board Backlog; each goes through flow-activate on pickup.

**Relevance:** Enables external analytics engines (Spark, Trino, DataFusion) to consume CQLite scan results as a table source; part of the connector-tier ecosystem that feeds OLAP workloads from CEP-11 plugin exports.

---

### #905: Compaction manager epic
**State:** OPEN · **Title:** Support a compaction manager

Structured epic (2026-07-05) after 8-pass research program (UCS 5.0-verified, lifecycle txn-log protocol, purge/overlap semantics, scheduler, standalone-tool template, reuse audit, prior art, product alignment). No ecosystem tool performs offline Cassandra SSTable merge; CQLite already has ~70% executor shipped. Phase B (planner + simulator, unblocked) has 4 children (#1983–#1986); Phase A′ (productize offline compactor, #1987, gated on #1537) has 1 child. **Status:** Phase A′ blocked by #1537; Phase B ready to activate.

**Relevance:** Compaction strategy selection and offline merge control essential for the standalone engine tier; directly enables node-local OLAP workload optimization without cluster coordination.

---

### #1336: RF-correct row-count stats (Flight/Trino optimizer)
**State:** CLOSED · **Title:** Flight/Trino: expose RF-correct (token-range-scoped) optimizer row-count stats

Closed issue following #944 (aggregation-pushdown gate via RF-invariant ratio). Goal was to expose RF-correct logical row-count to Trino optimizer without double-counting replicas across RF>1 keyspaces. Approach: token-range-scoped stats matching connector split assignment, aggregated to one logical token-space copy. **Status:** Spec-approved (Seam 1) and parked on branch issue-1336-rf-correct-row-stats (origin only); pickup = resume branch + flow-implement, P3.

**Relevance:** Improves Trino/DataFusion cost-optimizer accuracy when consuming Flight connector snapshots; enables data-aware query planning for OLAP workloads.

---

### #1440: panic=unwind build profile for bindings (DECIDED)
**State:** CLOSED · **Title:** panic=unwind build profile for binding cdylib artifacts

Closed (DECIDED 2026-07-01). Built a dedicated `panic = "unwind"` profile for Python/Node cdylib artifacts so PyO3/napi `catch_unwind` firewall re-activates; core panics become catchable exceptions instead of aborting the host interpreter. Workspace release keeps `panic = "abort"` for CLI/core. Complements (does not replace) core panic elimination in parser epic H. CLI/core use plain `--release` (abort); binding artifacts built with the new profile (unwind). **Status:** Decision approved; measurement step (binary-size/perf delta) pending implementation.

**Relevance:** FFI firewall safety critical to in-JVM JNI embedding option; allows safe Rust↔Java boundary crossing for CEP-11 plugin or standalone node-local scan engine without process abortion on core panic.

---

### #1406: CompressionInfo.db write claim boundary (DECIDED posture b)
**State:** CLOSED · **Title:** CompressionInfo.db write path: add fail-closed guard + claim boundary now, wire compression later

Closed (DECIDED 2026-07-01). CompressionInfo/CompressedDataWriter are built but unwired — no production write surface uses them; all tests are CQLite round-trip only (zero Cassandra validation). Decision (posture b): add guard test asserting public write surface CANNOT emit compressed SSTables; CLAUDE.md/parity manifest record "CQLite writes uncompressed SSTables" as claim boundary; defer full wiring to scoped M7 candidate. **Status:** Posture b accepted; guard + docs implementation needed.

**Relevance:** Establishes SSTable interchange contract between Cassandra cluster and standalone engine; compaction manager must respect uncompressed-only production boundary to avoid claim violations.

---

### #1959: Compaction finalize dir-fsync power-loss gap (P1)
**State:** OPEN · **Title:** Compaction finalize path has no directory fsyncs — power-loss reordering can break the TOC-existence crash-safety invariant

Open P1 bug found in #905 research. Compaction finalize (rename/delete sequences) lacks directory fsyncs, leaving power-loss window where OS may persist metadata out of order and violate TOC-existence crash-safety invariant (live/dead state = TOC.txt existence). Flush path fsyncs; compaction does not. Matches Cassandra 5.0 gap identified in lifecycle txn protocol. **Status:** Unfixed; blocks #1987 (offline compactor productize).

**Relevance:** Critical crash-safety for standalone compaction manager; power-loss during merge finalize must not corrupt SSTable inventory or introduce ghost components.

---

### #28: No-heuristics mandate (M1, CLOSED)
**State:** CLOSED · **Title:** Schema/comparator-only parsing: remove heuristics and blob fallbacks (P0)

Closed P0 M1 quality gate. Mandate: authoritative metadata only (no guessing), schema-aware decoding when schema present, CompressionInfo structured formats, legacy heuristics gated behind feature (not enabled in CI). Enforcement: header/format/compression detection, blob fallbacks, unit tests assert heuristic branches unreachable under CI config. **Status:** M1 complete.

**Relevance:** Foundational trust axiom for offline engine operation in plugin context; CEP-11 plugin must decode SSTables with explicit schema metadata only, never infer type/behavior from byte patterns.

---

## 10. Open forks (the epic's workstreams)

| WS | Fork | Shape of the answer |
|---|---|---|
| WS2 | Tail live-stream protocol | chunking, pin budget, snapshot fallback threshold, retry contract; §12.5: the tail tax is 89–97% of interactive (W1) latency — range-sliced/cached tail acquisition is the interactive-query story; verify range-bounded makePartitionIterator works under the readOrdering pin (§12.7) |
| WS3 | Generation pinning | tracker refs vs hardlink snapshots vs pinned manifest |
| WS4 | Process boundary | in-JVM JNI (#1440 firewall) vs companion process — decision packet |
| WS5 | Coordinator query surface | CQL verb vs node Flight + fan-out; freshness knob per request |
| WS6 | Cache design | envelope spec (multi-cell collections!), tiering, sizing; format per the §11.7 ladder |
| WS7 | Serde-cost sizing bench | `nb`-scan vs candidate cache formats (Parquet+zone-map sidecar, Vortex, Arrow IPC mmap) on real `nb`-converted segments with the actual envelope shape — the §11.7 missing-evidence list + §12.1 missing-measurements list (nb decode throughput, k-way merge ns/row, Parquet rate on our schema) |
| WS8 | OLTP-safety re-bench | G3 extended to live-stream mode |
| WS9 | CEP pitch (optional) | community story; precedents §3; unclaimed territory §7 |

**Exit criterion (epic #2037):** owner-approved decision packet — WS2–WS5 resolved on paper, WS7/WS8 numbers in hand, go/no-go for an implementation epic.

---

## 11. Beyond Parquet: the on-disk format survey for the columnar cache

### 11.1 The disposable-cache lens

The columnar cache is not the system of record — Cassandra's row-oriented `nb` SSTables stay authoritative, and every cache segment is derived, invalidated at compaction, and rebuilt lazily from `nb` data that CQLite already has to read correctly. That single fact inverts the usual format-selection calculus: lock-in risk is close to zero (a segment can be regenerated in a different format tomorrow with no migration, no dual-write period, no reader-compatibility contract to honor), so format-spec immaturity, pre-1.0 churn, and narrow ecosystem breadth — normally disqualifying for a storage format — become **weak** negatives here (criterion C8). What the evaluation should actually weight heavily is decode speed (C5), fine-grained random access along the token+clustering sort key (C2), true zero-copy Arrow interop for the hot tier (C1), and whether a Rust implementation is usable *today*, not on a roadmap (C6). Compression ratio, broad multi-engine ecosystem support, and long-term format stability — the properties that dominate a Parquet/Iceberg-style comparison — are secondary here precisely because rebuild cost, not storage footprint or reader longevity, is the operative cost function.

There is exactly one place this reverses: a **future cold tier** that gets shared with external lakehouse consumers (Spark/Trino/DuckDB reading CQLite-derived analytics output directly, or a durable export path) wants the boring, stable, universally-supported answer — Parquet, optionally under an Iceberg-style manifest — because at that point the artifact stops being disposable-to-CQLite-alone and starts being a contract with someone else's engine. The hot tier and the cold tier are allowed to make different format bets; conflating them is the mistake to avoid.

### 11.2 Comparison matrix

This survey covers general-purpose next-gen columnar containers, the established analytics formats, embedded-OLAP-engine native formats, time-series formats, and table-format metadata layers — evaluated as candidates for CQLite's disposable, regenerable per-generation cache segment.

| Format | Steward | Design center | C1 Arrow/mmap | C2 random access | C6 Rust today | Verdict for cache |
|---|---|---|---|---|---|---|
| **Lance (v2)** | LanceDB Inc. / independent `lance-format` org | Random-access-first columnar container, kills the Parquet row-group tradeoff | Deep Arrow integration + mmap zero-copy claimed | No row groups — adaptive per-column encodings give ≤1–2 IOPS point access | Yes — `lance`/`lance-file` crates, Rust-native, actively released | Strong hot-tier fit; range-slice-by-sort-key needs CQLite-side mapping, not a native zone map |
| **Vortex** | SpiralDB → LF AI & Data (Incubation) | Arrow-native cascading-compression format, explicit Parquet challenger | Zero-copy to Arrow by design; minimal footer enables mmap | Chunked layout + per-zone stats + `StructLayout` give sub-file, per-field pruning | Yes — `vortex-*` crates, Apache-2.0, active | Best-fit candidate overall: Arrow-native, chunk-seekable, streaming writer, wide-schema-friendly — but genuinely pre-1.0 |
| **Nimble** | Meta / `facebookincubator` | ML-feature-table format for 10k+-column sparse schemas | None confirmed — C++-only, no Arrow adapter, mmap undocumented | Block encoding for predictable memory; no documented fine-grained range-seek API | No — zero real Rust crate; only unrelated same-named crates exist | Wrong shape (column-count scale, not sort-key scans) and no Rust path today |
| **BtrBlocks** | TUM/FAU (Kuschewski, Sauerwein, Alhomssi, Leis), SIGMOD 2023 | Cascading lightweight-only compression for lake scans; decode-speed-first | No Arrow bindings or mmap story documented | 64,000-row fixed blocks, independently decompressible, but no delta encoding, no sub-block seek | No — C++ only, no Rust crate/bindings found (a `vortex-btrblocks` Rust crate exists as of the outside-the-box survey but is not the reference implementation) | Right philosophy, wrong host language — needs a full port or FFI |
| **FastLanes** | CWI (Afroozeh & Boncz), VLDB 2023 + VLDB 2025 | Fully data-parallel SIMD-auto-vectorizing compression + segmented file format for vector-level decode | No native Arrow interop documented (C++ reference); Rust crate is kernels-only | Best-in-class: 1,024-value vector native access granularity, engine can fetch compressed vectors directly | Partial — active Apache-2.0 `spiraldb/fastlanes` crate ports only the bit-packing/transposed-layout kernels, not the full file format | Closest design match to C2, but the full-format Rust story isn't there yet |
| **Apache ORC** | ASF (Hive ecosystem) | Write-heavy, ACID-transactional (Hive/Pig-oriented) | Limited — Arrow support is C++-only | Stripe (64–250MB) + 10K-row-group index with bloom filters; too coarse for token-range seeks | Immature — `orc-rust` (datafusion-contrib), read-only, no official arrow-rs support | Ecosystem/feature mismatch; ACID machinery is irrelevant to a disposable cache |
| **Apache Parquet** | ASF (Twitter/Cloudera; universal standard) | Read-optimized, broad-ecosystem analytics format | Excellent via arrow-rs; near-zero-copy, mmap-friendly | Page-level ColumnIndex + OffsetIndex (v1.11+) give effective min/max pruning down to the page | Mature — official arrow-rs/parquet, Apache-2.0, monthly releases, production-proven | Strong C1/C2/C5/C6 fit; Thrift footer overhead on wide/sparse schemas is a known but secondary weakness |
| **Arrow IPC File (Feather v2)** | ASF / arrow-rs | Fast, zero-copy columnar format for in-memory/mmap parity | Near-perfect zero-copy mmap; on-disk ≡ in-memory Arrow | Batch-level only — no column-level granularity within a batch; must decode the whole RecordBatch | Yes — production-grade arrow-rs, mature | Excellent hot-tier micro-cache; batch-only granularity is a real C2 gap for column-projected token-range scans |
| **DuckDB native format** | DuckDB Labs | Single-file block-based columnar, optimistic MVCC + lightweight compression | Zero-copy Arrow IPC/Feather interop; mmap-viable but not primary design center | Column-projection pushdown, 120K-row-group clustering; not built for key-range OLTP-style seeks | Yes — `duckdb-rs`, production-ready | Ecosystem lock-in (no external readers beyond DuckDB itself) precludes adoption as a shared cache format |
| **ClickHouse MergeTree part** | ClickHouse Inc. | Write-once, compression-first OLAP columnar | None — no zero-copy; Arrow conversion is client-protocol-side only | Granule-level (~8K rows) seeks via sparse index marks; decompresses the whole compression block per touch | No — native format explicitly not designed for external readers; Rust support is client-protocol-only | Proprietary/undocumented native format, per-column-file overhead scales badly on wide sparse schemas |
| **InfluxDB TSM** | InfluxData | Series-key organized blocks + index footer; dual-pass WAL+flush write | No mmap-safe zero-copy to Arrow; index is not Arrow-native | Strong within its own model (2-IOPS block access, binary-search index) but no per-column metadata, format predates Arrow | No — original in Go; `tsz-rs` covers Gorilla compression only, not TSM | Unsuitable: predates Arrow, dual-path write violates C3, block index has no room for C4's per-column metadata |
| **Gorilla (Meta)** | Facebook (archived) | In-memory time-series cache, XOR + delta-of-delta, write-through to HBase | None — in-memory cursor-oriented, no Arrow encoding, no mmap layout | None for token-range/clustering — series-sharded, full-series in-memory scan only | Limited — `tsz-rs` ports the codec only, not the system | Unsuitable on nearly every axis: in-memory-only, no token-range slicing, narrow (floats-only) codec scope |
| **QuestDB columnar** | QuestDB Inc. | Per-column append-only, time-partitioned, WAL-backed, with Parquet cold tiering | Partial — mmap-safe per-column files; Arrow conversion is not zero-copy | Strong for timestamp-ordered access; no token-range awareness (partitioning is time-only) | Moderate — `questdb-rs` is an ingestion client, not a storage-layer producer | Partially suitable pattern (per-column append-only fits C3) but needs real design work for token-range + envelope metadata |
| **Apache Iceberg** | Tabular.io → Databricks | ACID snapshots + hierarchical manifest stats over Parquet/ORC | Good — `iceberg-rs` 0.8.0 Arrow-native, mmap-able Parquet underneath | Excellent — manifest-list → per-file stats, Z-order clustering, page indexes | Yes — `iceberg-rs` 0.8.0, active, DataFusion-native | Manifest+stats pattern is worth stealing; full snapshot/ACID machinery is overkill for a disposable cache |
| **Delta Lake** | Databricks | Linear JSON transaction log + periodic Parquet checkpoint | Fair — checkpoints are Parquet, but Rust (`delta-rs`) is secondary to Python/Scala | Moderate — file-level stats only, no hierarchical pruning, worst case scans all active files | Emerging — `delta-rs` exists but lags the Java/Python-first implementation | Weak fit: ACID-commit-log model solves a concurrency problem the disposable cache doesn't have |
| **Apache Hudi** | Uber | Timeline-based multi-version instants + LSM archive (1.0) | Fair — Parquet underneath, but LSM compaction adds complexity | Moderate — file-group/timeline model complicates range filtering, merge-on-read cost | Emerging — Rust bindings exist but the codebase is Java-first | Weak fit: multi-version record tracking is irrelevant to an immutable-per-generation cache |
| **Apache Paimon** | Apache/Alibaba | Streaming-native LSM tree with bucket-scoped snapshots | Good — Parquet/ORC within LSM buckets, mmap candidate | Good in principle (LSM sort order) but multi-level merge-on-read adds query-time latency | Weak — Java-first, tight Flink coupling, Rust bindings non-standard | Weak fit: streaming-ingestion LSM overhead and Flink coupling invert C5's decode-speed priority |

### 11.3 Per-format detail

#### Lance (v2 format, LanceDB)

Lance v2 explicitly reframes the format as a "columnar container" rather than a Parquet-style fixed-layout file: it drops row groups entirely in favor of variable-length blocks and pushes all encoding logic into external, pluggable encoders that the reader/writer core knows nothing about, so new encodings ship without touching the file spec ([Lance v2: A New Columnar Container Format](https://blog.lancedb.com/lance-v2/), [mirrored](https://lancedb.com/blog/lance-v2/)). For **C2**, the arXiv paper backing this design reports Lance achieving random access "without sacrificing scan performance" via two adaptive encodings — full-zip transposition for wide/large values (≥128B) giving ~1 IOP/value, and mini-block 4–8KiB compressed chunks for narrow columns giving ~2 IOPS/value — versus Arrow-style encodings needing ~5 IOPS/value for something like `List<String>` ([arXiv:2504.15247](https://arxiv.org/html/2504.15247v1)); this is point/row-id random access and column projection, not a native sort-key zone-map, so CQLite would still need to exploit the fact that a per-generation segment is written in token order and map a token/clustering range to a contiguous row-offset range for the seek (unverified in the primary sources whether Lance ships embedded min/max stats for this). **C1** is satisfied directly: deep Arrow integration and mmap-based zero-copy access, with a deliberately compact (<50-line) protobuf schema. **C4**: column-level metadata (dictionaries, skip tables) is decoupled from data pages rather than duplicated per block, which the blog frames as a fix for sparse/wide schemas — a good structural match for per-column writetime/TTL i64 sidecars; nested list-of-struct support is addressed via "structural encodings" in the 2.1/2.2 line ([Lance File 2.1](https://blog.lancedb.com/lance-file-2-1-smaller-and-simpler/), [Lance File Format 2.2](https://www.lancedb.com/blog/lance-file-format-2-2-taming-complex-data)) though CQLite's separate range-tombstone stream would still be a bespoke side-channel, same as with any candidate. **C5**: mini-block/full-zip is explicitly decode-speed-first, and v2.2 claims "half the storage, none of the slowdown," suggesting the ratio/decode tradeoff has improved for a colder tier too ([Lance Format v2.2 Benchmarks](https://www.lancedb.com/blog/lance-format-v2-2-benchmarks-half-the-storage-none-of-the-slowdown)). **C3** (single-pass conversion from an immutable source) is asserted by "convert from Parquet in 2 lines of code" positioning but the write-path memory/pass guarantees are not independently verified — flag as inferred. **C6/C7**: Apache-2.0, Rust-native crates on crates.io, 6.8k GitHub stars, 528 releases, latest v8.0.0 as of 2026-07-01, governance moved out of the vendor's own org into an independent `lance-format` org with ASF/CNCF-style contributor→maintainer→PMC tiering ([Lance Community Governance](https://lance.org/community/), [github.com/lance-format/lance](https://github.com/lance-format/lance)) — healthy, though named third-party production users beyond LanceDB itself weren't confirmed. **C8**: unambiguously usable today from Rust; given the cache is disposable, the v2.0→v2.1→v2.2 spec churn is exactly the "weak negative" the criteria call out, not a blocker.

Sources: [Lance v2](https://blog.lancedb.com/lance-v2/) · [mirror](https://lancedb.com/blog/lance-v2/) · [arXiv:2504.15247](https://arxiv.org/html/2504.15247v1) · [Lance File 2.1](https://blog.lancedb.com/lance-file-2-1-smaller-and-simpler/) · [Lance File Format 2.2](https://www.lancedb.com/blog/lance-file-format-2-2-taming-complex-data) · [Lance v2.2 Benchmarks](https://www.lancedb.com/blog/lance-format-v2-2-benchmarks-half-the-storage-none-of-the-slowdown) · [Columnar File Readers in Depth: Structural Encoding](https://www.lancedb.com/blog/columnar-file-readers-in-depth-structural-encoding) · [Lance Community Governance](https://lance.org/community/) · [github.com/lance-format/lance](https://github.com/lance-format/lance) · [lance crate](https://crates.io/crates/lance)

#### Vortex (SpiralDB / LF AI & Data)

Vortex is built *around* Arrow rather than adapted to it: "all Arrow arrays can be converted into Vortex arrays with zero-copy, and a Vortex array constructed from an Arrow array can be converted back to Arrow, again with zero-copy" ([GitHub](https://github.com/vortex-data/vortex), [vortex.dev](https://vortex.dev/)) — directly satisfies **C1**. The footer/postscript is capped at 65,528 bytes with a segment map holding dictionary-encoded array/layout/compression specs, stated to specifically "enable memory mapping" for zero-allocation, zero-copy reads ([Vortex File Format spec](https://docs.vortex.dev/specs/file-format)).

For **C2**, random access is not row-group-only: the default writer progressively chunks arrays (8k-row chunks → 1MB uncompressed / ~2MB compressed flush) and attaches zone-map statistics per logical zone *independent of physical partitioning*, plus pluggable `ChunkedLayout`/`StructLayout`/`FlatLayout` primitives so column projection and sort-key-range pruning both operate below whole-file granularity ("[Towards Vortex 1.0](https://spiraldb.com/post/towards-vortex-10)"). `StructLayout` partitions a struct array into one child layout per field, and the FlatBuffer metadata format is explicitly built for "ultra-wide schemas (>>100k columns) with O(1) column access" ([docs.vortex.dev](https://docs.vortex.dev/)) — directly relevant to **C4**'s many-small-metadata-column shape; nested list-of-struct multi-cell collections map naturally onto struct/list encodings, though no source benchmarks CQLite's exact envelope shape (unverified).

**C3**: the writer is a bounded-memory streaming pipeline (chunk → sample via a BtrBlocks-inspired sampler → compress → buffer to ~2MB → flush) — a single forward pass with capped buffering, compatible with converting an immutable `nb` SSTable once. **C5**: the headline claim is compute-on-compressed-data via cascading lightweight encodings (FastLanes, FSST, ALP) rather than max ratio, with project-reported numbers claiming ~100-200x faster random-access reads and 2-10x faster scans than Parquet at "approximately the same compression ratio and write throughput as Parquet with zstd" ([GitHub](https://github.com/vortex-data/vortex)) — decode-speed-first, matching a disposable cache tier (note: these are vendor-reported, not independently reproduced numbers).

**C6/C7**: multiple native `vortex-*` crates on crates.io/lib.rs, Apache-2.0, now an LF AI & Data Incubation-stage project with a documented Technical Charter — real governance ([lfaidata.foundation/projects/vortex](https://lfaidata.foundation/projects/vortex/)). Integrations claimed for "Arrow, DataFusion, DuckDB, Spark, Pandas, Polars, & more," with Iceberg "coming soon" ([GitHub README](https://github.com/vortex-data/vortex)); named early adopter Spice AI ([spice.ai/blog](https://spice.ai/blog/vortex-at-spice-ai-the-columnar-format-for-data-intensive-workloads)), but broader production evidence is thin — treat as emerging, not established.

**C8**: usable today from Rust — the on-disk file format has been considered stable with backwards compatibility "from v0.36.0 onward," using dated `YYYY.MM.DD` editions (forward-compat via embedded WASM is *planned but not yet implemented*). Given the cache is disposable, this pre-1.0 status and "Library APIs may change from version to version" (per GitHub) is explicitly a **weak** negative — segments can be rebuilt in a newer edition after any breaking bump.

Sources: [vortex-data/vortex](https://github.com/vortex-data/vortex) · [vortex.dev](https://vortex.dev/) · [Vortex File Format spec](https://docs.vortex.dev/specs/file-format) · [Vortex docs](https://docs.vortex.dev/) · ["Towards Vortex 1.0"](https://spiraldb.com/post/towards-vortex-10) · [LF AI & Data project page](https://lfaidata.foundation/projects/vortex/) · [Vortex at Spice AI](https://spice.ai/blog/vortex-at-spice-ai-the-columnar-format-for-data-intensive-workloads) · [vortex-dtype/vortex-roaring/vortex-error/vortex-scan — lib.rs](https://lib.rs/crates/vortex-dtype) · [Show HN: Vortex](https://news.ycombinator.com/item?id=41839773)

#### Nimble (Meta, formerly Alpha)

Nimble (née "Alpha") is Meta's from-scratch columnar format ([facebookincubator/nimble](https://github.com/facebookincubator/nimble/)), presented at VeloxCon 2024 ([talk](https://www.youtube.com/watch?v=bISBNVtXZ6M); [HN recap](https://news.ycombinator.com/item?id=39995112)). Its design center is ML feature-engineering/training tables with **thousands to tens of thousands of columns** — the opposite axis from CQLite's need (modest per-row metadata columns, deep sort-key range scans over few-hundred-column CQL tables), per the [README](https://github.com/facebookincubator/nimble/blob/main/README.md) and [Meta's engineering blog](https://engineering.fb.com/2024/05/22/data-infrastructure/composable-data-management-at-meta/). It replaces stream-oriented layout with **block encoding** for "predictable memory usage while decoding/reading" and uses FlatBuffers instead of Thrift/Protobuf specifically to keep overhead down at thousands-of-columns scale — no source confirms or denies embedded per-block statistics (C2/C5 unconfirmed either way). The headline feature is a cascading/recursive, pluggable encoding scheme (relevant to C5, but no independent Rust benchmark exists vs. Parquet/Lance).

Language surface is **C++ only**, tightly coupled to Velox (git submodule); there is **no Arrow adapter, no mmap discussion, no fine-grained range-seek API** documented anywhere found — a material C2 gap for token-range-scoped scans. On C6, the top crates.io/docs.rs hits for "nimble" (`nimble`, `nimble-rust`, `apache-nimble-sys`, `esp32-nimble`) are confirmed **unrelated projects** ([docs.rs/nimble](https://docs.rs/nimble), [crates.io/nimble-rust](https://crates.io/crates/nimble-rust)) — genuinely zero Rust bindings today. License is Apache-2.0 (favorable); **C8** — since spec churn is a weak negative for a disposable cache — is actually Nimble's strongest point: it openly states "no stability or versioning guarantees (yet)" and has shipped no tagged releases, which would disqualify a durable format but is tolerable here. The practical blocker is C++-only, not Rust, today. Net: worth stealing the cascading-encoding idea conceptually, but fails C1/C2/C6 outright as a directly adoptable crate.

Sources: [facebookincubator/nimble](https://github.com/facebookincubator/nimble/) · [README](https://github.com/facebookincubator/nimble/blob/main/README.md) · [Composable data management at Meta](https://engineering.fb.com/2024/05/22/data-infrastructure/composable-data-management-at-meta/) · [VeloxCon talk](https://www.youtube.com/watch?v=bISBNVtXZ6M) · [HN discussion](https://news.ycombinator.com/item?id=39995112) · [HN discussion 2](https://news.ycombinator.com/item?id=40163530) · [Nimble and Lance: The Parquet Killers](https://materializedview.io/p/nimble-and-lance-parquet-killers) · [docs.rs/nimble (unrelated crate)](https://docs.rs/nimble) · [crates.io/nimble-rust (unrelated crate)](https://crates.io/crates/nimble-rust)

#### BtrBlocks

BtrBlocks (SIGMOD 2023, [paper](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf), [repo](https://github.com/maxi-k/btrblocks), MIT) targets exactly CQLite's C5 thesis: it optimizes for scan cost/throughput over compression ratio via cascaded lightweight schemes (RLE, dictionary, frequency, FOR+bitpacking via SIMD-FastPFOR/FastBP128, FSST, Roaring, "Pseudodecimal" float encoding), reporting 2.2x faster scans and 1.8x cheaper than Parquet+Zstd on the five largest Public BI datasets. **C2**: columns divide into fixed 64,000-entry blocks that compress/decompress independently and in parallel, with metadata/statistics stored orthogonally so a reader can prune blocks via zone maps before touching payload — but the FastLanes authors note BtrBlocks "completely avoids delta encoding" and decompresses at "the full rowgroup level, imposing a large memory footprint" ([FastLanes paper](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf), §1.1) — seeks are block-granular (64K rows), not page/vector-granular. **C3**: sampling-based scheme selection, block-independent compression, consistent with a single streaming pass per block, though the paper gives no explicit write-side memory numbers. **C6/C7**: maintained-but-academic MIT-licensed C++ repo (285 stars, 3 core contributors) with **no Rust implementation** — unverified whether anyone has started one. **C8**: fully usable today, but only from C++; adoption means FFI-wrapping or re-implementing the encoding cascade natively in Rust.

Sources: [BtrBlocks paper (PDF)](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) · [maxi-k/btrblocks](https://github.com/maxi-k/btrblocks) · [ACM DOI](https://dl.acm.org/doi/10.1145/3589263) · [FastLanes File Format, §1.1 comparison](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf)

#### FastLanes

FastLanes is CWI's two-generation research program, and it maps onto CQLite's criteria almost point-for-point. The 2023 VLDB paper ([Afroozeh & Boncz](https://dl.acm.org/doi/10.14778/3598581.3598587)) introduced a virtual 1024-bit interleaved "Unified Transposed Layout" that lets scalar (non-intrinsic) code auto-vectorize bit-unpacking, FOR, delta, and RLE decode, reportedly decoding >100 billion integers/sec — a direct, verified instance of "decode speed >> ratio" (C5). The 2025 VLDB paper, ["The FastLanes File Format"](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf) (artifact at [ir.cwi.nl/pub/35881](https://ir.cwi.nl/pub/35881/35881.pdf)), turns this into a full file format explicitly contrasted against Parquet and BtrBlocks in its own Table 1: access granularity is a 1K-value vector (vs. Parquet's 1MB chunk, BtrBlocks' 64K rowgroup), and — uniquely among the three — the API can **return compressed vectors** to the query engine for compressed execution, directly serving C2's page/block-level-seek requirement. The "Segmented Page Layout" stores each column-chunk's encoded expression (chained operators like `FFOR`, `DELTA`, `DICT`, `FSST`, `RLE`) with an explicit entry-points array per vector — true fine-grained seek without decoding surrounding vectors, relevant to C4's per-column metadata and sparse-schema concerns since each column/expression is independently addressable. C3 is implied by the vectorized, streaming design but not benchmarked explicitly. **C6/C7**: the reference file-format implementation is C++ (MIT, "absolutely zero code dependencies"); the separately-maintained Rust crate ([spiraldb/fastlanes](https://github.com/spiraldb/fastlanes), Apache-2.0, active) reimplements only the bit-packing/transposed-layout kernels — reordered for fused-kernel performance and explicitly **not binary-compatible** with the CWI reference — consumed by Vortex rather than being a drop-in FastLanes-file-format reader. **C8**: the core codec is usable from Rust today; the full segmented file format with expression encoding is not — extending the Rust crate to file-format level or FFI-ing the C++ reference are both bounded efforts given the cache can absorb a simpler first-cut layer.

Sources: [FastLanes File Format (VLDB 2025, PDF)](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf) · [FastLanes Compression Layout (VLDB 2023)](https://dl.acm.org/doi/10.14778/3598581.3598587) · [CWI IR record, compression layout](https://ir.cwi.nl/pub/33289) · [spiraldb/fastlanes (Rust crate)](https://github.com/spiraldb/fastlanes) · [CWI IR record, file format](https://ir.cwi.nl/pub/35881/35881.pdf)

#### Apache ORC

ORC is architecturally Hive-centric, designed around ACID transactional writes irrelevant to Cassandra's cache. The [three-level indexing](https://orc.apache.org/docs/indexes.html) (file/stripe/10K-row-group) includes [bloom filters (Hive 1.2+)](https://orc.apache.org/docs/indexes.html), but stripes (default 64–250MB) are too coarse for token-range seeks on C2. [Arrow integration exists only in C++](https://arrow.apache.org/docs/cpp/orc.html), not Rust; [orc-rust](https://docs.rs/orc-rust) (datafusion-contrib, read-only, planned write support) is the sole Rust implementation and [markedly less mature](https://github.com/datafusion-contrib/orc-rust) than parquet-rs. The ACID feature set is irrelevant to a regenerable per-generation cache. **Verdict**: Hive-centric design, coarse stripe granularity, and immature Rust stack make ORC unsuitable for token-range-scoped filtering; the ACID machinery adds zero value to a disposable segment.

Sources: [ORC Indexing](https://orc.apache.org/docs/indexes.html) · [ORC Specification v1](https://orc.apache.org/specification/ORCv1/) · [orc-rust crate](https://docs.rs/orc-rust) · [Arrow C++ ORC support](https://arrow.apache.org/docs/cpp/orc.html)

#### Apache Parquet

[Parquet v2 data pages](https://parquet.apache.org/docs/file-format/data-pages/encodings/) separate repetition/definition levels from data, allowing selective compression (C5). [Byte-stream-split encoding](https://parquet.apache.org/docs/file-format/data-pages/encodings/) improves downstream compression effectiveness without inflating page size. **C2 is a strength**: [page-level column indexes (v1.11+)](https://parquet.apache.org/docs/file-format/pageindex/) store min/max per page; [Athena demonstrates 10x+ scan reduction](https://aws.amazon.com/blogs/big-data/how-to-use-parquet-column-indexes-with-amazon-athena/) by skipping pages within row groups — a good match for token-range + column-projection filtering. **C1**: [arrow-rs/parquet](https://arrow.apache.org/rust/parquet/) provides zero-copy reads via columnar mmap; [2025 improvements](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/) introduced a custom Thrift parser giving a 3x metadata-decode speedup, enabling selective field parsing. **C6**: official arrow-rs implementation, monthly releases, v57+, Apache-2.0, production users. **C4 caveat**: the Thrift-based footer creates [overhead with wide schemas](https://starburst.io/blog/parquet-orc-machine-learning/) (metadata decode scales with column count); a Flatbuffer-footer proposal is in progress. For CQLite's envelope, Parquet's columnar layout is a natural fit; Thrift overhead is a documented weakness but secondary given cache regenerability. **C3/C8**: single-pass streaming write via [AsyncArrowWriter](https://docs.rs/parquet/latest/parquet/arrow/arrow_writer/); format is stable and usable today. **Verdict**: page-level indexing, mature Rust ecosystem, zero-copy Arrow, and a read-optimized decode path align strongly with C1/C2/C5/C6; footer overhead on sparse wide schemas is real but secondary to a regenerable cache.

Sources: [Parquet Page Index](https://parquet.apache.org/docs/file-format/pageindex/) · [Byte-Stream-Split Encoding](https://parquet.apache.org/docs/file-format/data-pages/encodings/) · [Arrow Rust Parquet](https://arrow.apache.org/rust/parquet/) · [Parquet Metadata Parsing 3x Speedup](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/) · [AWS Athena Column Index Filtering](https://aws.amazon.com/blogs/big-data/how-to-use-parquet-column-indexes-with-amazon-athena/) · [Parquet vs ORC for ML](https://starburst.io/blog/parquet-orc-machine-learning/) · [Parquet License](https://parquet.apache.org/docs/asf/license/)

#### Arrow IPC File (Feather v2)

The [Arrow IPC File Format](https://arrow.apache.org/docs/format/Columnar.html) encodes RecordBatches with a footer of batch-level offsets, enabling random access to any batch without a sequential scan (C1/C2 at batch granularity). Buffers are 8-byte aligned for direct mmap read-as-written ([Ursa Labs Feather v2 benchmark](https://ursalabs.org/blog/2020-feather-v2/)); zero-copy deserialization needs no type translation because on-disk layout mirrors in-memory Arrow arrays. Compression is per-batch (LZ4/ZSTD); uncompressed files run 3–10x larger than Parquet (acceptable per C5 for a disposable cache). **Critical limitation (C2)**: column projection is **not** supported — accessing a subset of columns from a batch requires deserializing the entire RecordBatch; no page/column-level indexes exist. arrow-rs ([crates.io](https://crates.io/crates/arrow), [releases](https://github.com/apache/arrow-rs/releases)) is ASF-governed, Apache-2.0, production-mature, with `FileWriter` supporting single-pass streaming write of nested Struct/List types and custom metadata (C3/C4 fit). **C8**: usable today, format finalized and backward-compatible. **Verdict**: strong for hot-tier micro-caching, but batch-level-only random access is a real gap for coordinator token-range filtering with column projection — full batches decode even for single-column scans. This directly motivates §11.6's "Arrow IPC + sidecar zone-map index" direction rather than treating raw IPC as sufficient on its own.

Sources: [Arrow v24.0.0 Columnar Format Spec](https://arrow.apache.org/docs/format/Columnar.html) · [Feather V2 with Compression — Ursa Labs](https://ursalabs.org/blog/2020-feather-v2/) · [Apache Arrow — crates.io](https://crates.io/crates/arrow) · [Arrow Rust GitHub Releases](https://github.com/apache/arrow-rs/releases) · [arrow-rs FileWriter docs](https://docs.rs/arrow-ipc/latest/arrow_ipc/writer/struct.FileWriter.html)

#### DuckDB native format

[DuckDB's storage format](https://duckdb.org/docs/current/internals/storage) uses fixed 256KB blocks organized into [row groups of ~122,880 rows](https://www.alibabacloud.com/blog/duckdb-internals---part-1-file-format-overview_602511), with [lightweight compression](https://duckdb.org/2022/10/28/lightweight-compression) (FSST, RLE, dictionary, bit packing, FOR, Chimp/Patas, Zstd). [Arrow interop is zero-copy](https://duckdb.org/2021/12/03/duck-arrow) via Arrow IPC; filters/projections push down into scans. Write path uses [optimistic concurrency with a transactional WAL](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions): bulk updates write new blocks directly and reference them in the WAL — near-zero-cost transactionality, satisfying C3 for single-pass append. [Format stability guaranteed from v1.0 onward](https://duckdb.org/docs/current/internals/storage) (C8 verified). **Critical limitation**: the format is **ecosystem-locked** — [no production external readers exist outside the DuckDB ecosystem](https://duckdb.org/docs/current/guides/file_formats/read_duckdb); the docs themselves [recommend Parquet or Arrow IPC](https://motherduck.com/learn/columnar-storage-guide/) for cross-system portability. CQLite's cache needs zero-copy handoff to Arrow consumers and Flight reads; DuckDB's format precludes this without bundling its C++ library — trades ecosystem reach for performance, unsuitable as a shared cache layer.

Sources: [DuckDB Storage Format & Internals](https://duckdb.org/docs/current/internals/storage) · [Lightweight Compression](https://duckdb.org/2022/10/28/lightweight-compression) · [DuckDB Quacks Arrow](https://duckdb.org/2021/12/03/duck-arrow) · [Analytics-Optimized Transactions](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions) · [Reading DuckDB Files](https://duckdb.org/docs/current/guides/file_formats/read_duckdb) · [Alibaba: DuckDB Internals Part 1](https://www.alibabacloud.com/blog/duckdb-internals---part-1-file-format-overview_602511) · [duckdb-rs crate](https://crates.io/crates/duckdb)

#### ClickHouse MergeTree part format

MergeTree stores each column in a separate `.bin` file (wide format) or bundled `data.bin` (compact format, <10MB), paired with a `.mrk3` mark file of uncompressed granule-boundary offsets [1]. The sparse primary index (`primary.idx`) holds one entry per 8,192-row granule [2], enabling O(log n) granule selection; random access requires decompressing the whole compression block containing the target granule [3]. Compression is two-stage: specialized encodings (Delta, DoubleDelta, Gorilla, T64) precede LZ4/ZSTD [4][5]. The native format is **explicitly not designed for external tools** — ClickHouse's own docs state "it doesn't make sense to work with this format yourself" [6]. Rust support exists only for client-side consumption (`clickhouse-arrow` converts query results via the native protocol, not by reading disk files) [7]. Per-column file overhead scales poorly with wide, sparse schemas — Cassandra's many timestamp/TTL metadata columns would explode file count. For a disposable cache, the write-once semantics and heavy compression are overkill, and proprietary lock-in contradicts the goal of a transparent, regenerable cache layer.

Sources: [1] [ClickHouse Data Storage Handbook](https://posthog.com/handbook/engineering/clickhouse/data-storage) · [2] [Sparse Primary Indexes](https://clickhouse.com/docs/guides/best-practices/sparse-primary-indexes) · [3] [MergeTree Mark File Format](https://vutr.substack.com/p/i-spent-8-hours-learning-the-clickhouse) · [4] [ClickHouse Compression Codecs](https://clickhouse.com/docs/data-compression/compression-in-clickhouse) · [5] [Gorilla/Delta/T64 Codec Details](https://clickhouse.com/blog/optimize-clickhouse-codecs-compression-schema) · [6] [Native Format Limitations](https://oneuptime.com/blog/post/2026-03-31-clickhouse-choose-data-format-view) · [7] [clickhouse-arrow (client protocol only)](https://docs.rs/clickhouse-arrow)

#### InfluxDB TSM

TSM arranges data in header/blocks(CRC32)/index(sorted by series-key, field-key)/footer sections. Blocks are independently decompressible and randomly addressable at 2 IOPS regardless of file size; a binary-search index avoids loading the full index into memory. Write path is dual-track: WAL + in-memory cache simultaneously, flushed to closed TSM files, triggering compaction — **a C3 violation** (cache flush compresses a snapshot, not a single streaming pass from the source SSTable). Compression is adaptive per type (RLE for regular-cadence timestamps, XOR for floats, bit-packing for integers). **C4 violation**: per-block index entries store only offset/size/min_time/max_time — no per-column TTL/writetime metadata. License is fully open (MIT/Apache 2.0). InfluxDB v3 abandoned TSM entirely for Arrow + DataFusion + Parquet — a signal the format's impedance mismatch with modern columnar systems is recognized by its own stewards. **Verdict**: unsuitable — predates Arrow, dual-pass write, block-level index too coarse for C4's metadata needs.

Sources: [InfluxDB TSM Kaitai Struct spec](https://formats.kaitai.io/tsm/index.html) · [InfluxDB Storage Engine design doc](https://github.com/influxdata/influxdb/blob/master/tsdb/engine/tsm1/DESIGN.md) · [InfluxDB v3 architecture](https://www.influxdata.com/blog/influxdb-3-oss-ga/) · [TSM write/compression efficiency](https://www.influxdata.com/blog/improved-data-ingest-compression-influxdb-3-0/)

#### Gorilla (Meta/Facebook)

Gorilla compresses timestamps via delta-of-delta (96% of regular-cadence timestamps → 1 bit) and values via XOR (51% identical values → 1 bit). Two-hour blocks are the persistence unit and lack per-column metadata. Distribution is share-nothing per-series sharding; write path appends to an in-memory unordered map, syncs to GlusterFS (write-through), tails older blocks to HBase — **a C3 violation** (multi-pass, not single streaming write from an `nb` SSTable). **C2 critical miss**: sharding assumes series-key locality; token-range + clustering-key scans are not supported. **C4 violation**: no TTL/writetime metadata; XOR codec is float-only, too narrow for CQL's 12 numeric types. Archived, unmaintained since 2018. **Verdict**: in-memory architecture is fundamentally incompatible with a disk-resident cache; no token-range slicing; codec scope too narrow; producer complexity (GlusterFS + HBase) unsuitable for embedding.

Sources: [Gorilla (VLDB 2015 paper)](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) · [Morning Paper summary](https://blog.acolyer.org/2016/05/03/gorilla-a-fast-scalable-in-memory-time-series-database/) · [tsz-rs (codec only)](https://github.com/jeromefroe/tsz-rs) · [Beringei (archived)](https://github.com/facebookarchive/beringei)

#### QuestDB columnar format

QuestDB stores each column as a separate append-only, mmap-able file within time-based partitions, compressed per-column (delta for timestamps, bit-packing for numbers, dictionary for strings). Ingestion writes to WAL then applies to the native columnar tier; older partitions auto-tier to Parquet. Queries specify a time range + column list; only relevant partitions/column files are touched. **C2 caveat**: seeks are timestamp-ordered only — CQLite's token-range + clustering-key filtering would need added partition-key metadata or a secondary index (not built in). **C4 gap**: no per-row/per-element metadata (TTL, writetime) support natively; the Parquet tier could add columns but at conversion cost. **C3 win**: single-pass append to per-column files fits a streaming producer well. Core engine is Java/C++; `questdb-rs` is a write client only, not a storage-layer producer. **Verdict**: the per-column append-only + mmap pattern is sound, but token-range + metadata-envelope handling requires real design work, and the Rust ecosystem gap means a producer would have to be built entirely in CQLite's own stack — comparable in spirit to a simplified, partitioned Arrow IPC stream with mmap.

Sources: [QuestDB Storage Engine architecture](https://questdb.com/docs/architecture/storage-engine/) · [QuestDB Columnar File Format glossary](https://questdb.com/glossary/columnar-file-format/) · [questdb-rs](https://crates.io/crates/questdb-rs) · [QuestDB hybrid row-columnar + Parquet tiering](https://questdb.com/blog/time-series-olap-lakehouse-questdb-architecture/)

#### Table-format layers: Iceberg, Delta Lake, Hudi, Paimon

Iceberg, Delta Lake, Hudi, and Paimon are **metadata/manifest layers over columnar file formats** (Parquet, ORC), not file formats themselves — they provide transaction/snapshot/deletion tracking above immutable data files. For a disposable per-generation cache this distinction matters: a full table format is likely overkill, but the **manifest + stats + sidecar-metadata pattern** is worth borrowing.

**Apache Iceberg.** The four-level hierarchy ([metadata.json → manifest_list → manifest files → data files](https://iceberg.apache.org/spec/)) progressively narrows search space; each manifest file is itself a Parquet table of per-data-file tuples (path, partition spec, column stats, record count, and, in v3, deletion-vector references via Puffin sidecar offsets). **C2 strength**: manifest-level pruning + Z-order clustering + Parquet page indexes give multi-level range skipping without decoding. **C3**: writer-side is single-pass; manifest emission is offline stats aggregation. **C1**: `iceberg-rs` 0.8.0 (Jan 2026) added native Arrow schema conversion and partition-aware `FileScanTask` → `RecordBatch`; arrow-rs gives zero-copy mmap for the underlying Parquet. **C6**: production-grade — 37 contributors, DataFusion integration, Trino/Snowflake/BigQuery drivers all ship Iceberg. **Verdict**: the manifest-as-stats-table pattern is strong for C2 (per-file bounds + clustering metadata benefit token-scoped scans); the Puffin sidecar pattern is elegant for a range-tombstone envelope. But Iceberg's snapshot/ACID/time-travel machinery is architectural overkill — CQLite's cache is disposable and regenerated per SSTable generation, not multi-version. A tiny generation-keyed manifest (one Parquet row per data file, rewritten wholesale on invalidation) borrows the structure at a fraction of the machinery.

**Delta Lake.** `_delta_log/` is an append-only sequence of JSON action files, checkpointed to Parquet every 10 commits by default; readers reconstruct state from the latest checkpoint plus subsequent JSON. **C2**: checkpoints carry file-level stats but **lack hierarchical pruning** — no per-file column min/max exposed in the log format, so planning worst-cases to a full active-file scan. **C3**: single-JSON-append writes are fine, but the log grows unboundedly and checkpoint compaction is a separate offline step. **C1**: checkpoints are Parquet, but public Rust support (`delta-rs`) is secondary to Python/Scala. **Verdict**: weak fit — the transaction-log model solves ACID durability under concurrent writers, a problem CQLite's cache (regeneration is atomic, single-threaded) doesn't have; C2 stays weak without hierarchical stats, and checkpoint compaction is unsuitable for a cache invalidated and rebuilt frequently.

**Apache Hudi.** Tracks every operation in a `.hoodie/` timeline (active + LSM-archived instants in Hudi 1.0); data lives in file groups where all versions of a record map to one group. **C2**: file-group-level indexing is complex — a record's latest version may span base + delta files requiring merge-on-read, complicating range filtering. **C3**: writes buffer in memory then land as small files, with background compaction merging file groups — two-pass in spirit, not single-pass. **C1**: data is Parquet/ORC but Rust support is immature (thin wrappers over a Java-first design). **Verdict**: weak fit — the file-group + timeline model is optimized for multi-version record history, which a disposable, immutable-per-generation cache doesn't need; compaction and record-lineage tracking add unjustified overhead, and the LSM archive trades query-time merge-on-read latency for storage savings, the opposite of C5's decode-speed priority.

**Apache Paimon.** Integrates LSM-tree levels per partition bucket (memtable → L0 → L1 → ...), snapshotting each bucket independently; readers merge across LSM levels at query time. **C2**: sort order within each level enables range skipping by key bounds, but multi-level merge-on-read adds query-time latency and CPU. **C3**: write-side is single-pass to memory, but flush-to-LSM is write amplification, not truly single-pass to disk. **C1**: data is Parquet/ORC within LSM files, but Rust support is immature and Flink-coupled. **Verdict**: weak fit — the streaming-ingestion design targets high-frequency continuous compaction, which a cache regenerated once per SSTable generation doesn't need; bucket-scoped snapshot multiplicity is a poor match for "invalidate = drop the whole generation's buckets," and LSM merge-on-read again inverts C5.

**Synthesis.** These four are full table formats bundling ACID/concurrency/multi-version semantics CQLite's disposable cache doesn't need — no snapshots, transaction logs, deletion vectors, LSM compaction, or time-travel are required when one generation is one immutable cache state. Two patterns are worth stealing regardless of which base container format is chosen: (1) Iceberg's **manifest-as-table** pattern — a lightweight, per-generation Parquet manifest of per-file min/max/null_count/record-count stats, rewritten atomically on invalidation, enabling token-range-scoped projection without decoding (C2/C4), at O(num-data-files) overhead; (2) the **Puffin sidecar-file** pattern — range tombstones and multi-cell writetime/TTL envelopes living in a small adjacent binary file referenced by offset/size from the manifest, avoiding exploding the main data files with envelope columns (C4). The underlying file format stays agnostic and swappable — Parquet today, replaceable tomorrow, since the cache is regenerable.

Sources: [Apache Iceberg Specification](https://iceberg.apache.org/spec/) · [Iceberg Rust 0.8.0 Release](https://iceberg.apache.org/blog/apache-iceberg-rust-0.8.0-release/) · [Iceberg v3 Deletion Vectors on AWS](https://aws.amazon.com/blogs/big-data/accelerate-data-lake-operations-with-apache-iceberg-v3-deletion-vectors-and-row-lineage/) · [Metadata Structure of Modern Table Formats](https://medium.com/data-engineering-with-dremio/the-metadata-structure-of-modern-table-formats-33b97f7828a1) · [Delta Lake Transaction Log Architecture](https://www.databricks.com/blog/2019/08/21/diving-into-delta-lake-unpacking-the-transaction-log.html) · [Transaction Log Structure (Medium)](https://medium.com/@nidhin.dwh/inside-the-delta-lake-transaction-log-the-invisible-engine-that-makes-lakehouse-architecture-reliable-5478dd41d17e) · [Apache Hudi LSM Timeline (1.0)](https://hudi.apache.org/blog/2025/05/29/lsm-timeline/) · [Timeline Architecture (Medium)](https://medium.com/@sanjeets1900/apache-hudi-timeline-lsm-tree-timeline-a48d7f7720ba) · [Apache Paimon Real-Time Pipeline Design](https://celerdata.com/glossary/building-a-real-time-data-pipeline-with-apache-paimon) · [The Ultimate Guide to Open Table Formats](https://dev.to/alexmercedcoder/the-ultimate-guide-to-open-table-formats-iceberg-delta-lake-hudi-paimon-and-ducklake-dnk) · [Iceberg vs Parquet: Table Format vs File Format](https://risingwave.com/blog/iceberg-vs-parquet-table-format-file-format/) · [Metadata Layer Architecture of Open Table Formats](https://www.pracdata.io/p/metadata-layer-design-of-open-table-formats)

### 11.4 HTAP/live-analytics precedents

These are the closest published precedents to CQLite's coordinator-native thesis: an OLTP row store stays authoritative, and a columnar representation is built *from* it — synchronously (delta-merge) or via async replication (learner replica) — with a clean split between a write-optimized recent layer and a read-optimized bulk layer. That two-layer shape (small write-friendly staging area, periodically compacted into read-optimized columnar) structurally mirrors "`nb` SSTable → disposable columnar cache."

- **SAP HANA — "Efficient Transaction Processing in SAP HANA Database: The End of a Column Store Myth."** Sikka, Färber, Lehner, Cha, Peh, Bornhövd. SIGMOD 2012, pp. 731–742. Introduces the **delta store / main store** split: writes land in a small, row-like, write-optimized delta store; a background **delta merge** periodically folds the delta into the heavily dictionary-compressed, read-optimized main columnar store. Relevant because it's the canonical proof that a database can present a single logical table while physically forking recent-vs-settled data into different physical layouts, with the merge itself being the expensive, poolable operation — directly analogous to "recompute the columnar cache at compaction, not per-query."

- **SingleStore — "Cloud-Native Transactions and Analytics in SingleStore."** Prout, Wang, Victor, Sun, Li, Chen, Bergeron, Hanson, Walzer, Gomes, Shamgunov. SIGMOD 2022 (industry track). Describes **Universal Storage**: each columnstore partition is physically an LSM tree whose top level is an in-memory rowstore (absorbs high-throughput writes) and whose lower levels are compacted, HTAP-optimized columnar segments; seeks into the columnstore use secondary indexes to avoid full scans. Validates LSM-shaped "recent-writes-as-rows, bulk-as-columns" as production-grade, not just academic, and the rowstore→columnstore compaction is exactly the trigger CQLite uses (SSTable flush/compaction) to regenerate its cache.

- **TiDB / TiFlash — "TiDB: A Raft-Based HTAP Database."** Huang et al. VLDB 2020, PVLDB 13(12), pp. 3072–3084. TiFlash is a **Raft learner** replica — it receives the Raft log asynchronously (no vote, no commit-path latency added to the OLTP leader) and converts row-format replicated tuples into columnar storage. *Unverified detail (sourced from PingCAP engineering blogs, not confirmed against the VLDB paper text itself):* TiFlash's storage engine, **DeltaTree**, keeps each Segment split into a **Delta Layer** (append-ordered, write-optimized, ~5% of a segment's data) and a **Stable Layer** (the bulk, read-optimized column data), with periodic compaction of deltas into the stable layer; TiFlash built DeltaTree instead of reusing ClickHouse's MergeTree because MergeTree doesn't natively support the high-frequency point updates a Raft-learner columnar replica must absorb. This is the single closest architectural analogue to CQLite's proposal: an async, node-local columnar replica derived from an authoritative row-oriented log, invalidated/rebuilt in layers rather than treated as a monolithic index. Worth a deeper read of DeltaTree directly from PingCAP's engineering blog (`pingcap.com/blog`, `tidb.net/book`) since the primary paper's storage-engine section is thin on internals.

What this precedent set validates for CQLite: the two-layer write-side/read-side split is a proven, production-grade pattern across three independent systems (an in-memory academic-turned-commercial engine, a cloud OLTP+OLAP hybrid, and a distributed SQL system), and in TiFlash's case the trigger for columnar regeneration is exactly the same shape as CQLite's — an async, node-local derivation off an authoritative replicated log, rebuilt incrementally rather than atomically per-query. What none of them supply: **Cassandra semantics.** None of HANA's delta-merge, SingleStore's Universal Storage, or TiFlash's DeltaTree carry tombstone-correct merge across an LSM of immutable SSTables, TTL expiry, or `nb`'s specific on-disk row/cell encoding — they are all columnar layers bolted onto *their own* engine's row store, not a pluggable columnar cache designed to sit beside someone else's storage engine. That is the actual novelty of CQLite's coordinator-native proposal: none of these systems are — or were ever designed to be — an external, pluggable analytics cache for a *different* database's storage format.

### 11.5 Research bibliography

- **"An Empirical Evaluation of Columnar Storage Formats."** Zeng, Hui, Shen, Pavlo, McKinney, Zhang. PVLDB 17(2): 148–161. *(Note on year: DOI record dates it 2023 in vol. 17 no. 2, but vol. 17 papers were presented at VLDB 2024 — cite as "VLDB Endowment 2023/VLDB 2024" to avoid ambiguity.)* Reopens Parquet's and ORC's internals and finds several default design choices are now wrong for modern hardware/workloads: dictionary encoding should be on by default, integer encoders should favor **decode speed over compression ratio**, block-level heavyweight compression (Snappy/Zstd wrapping) should be optional rather than mandatory, auxiliary structures (offsets, statistics) should be finer-grained. **Directly load-bearing**: legitimizes trading ratio for decode speed as the correct default for a repeatedly-scanned columnar layer — precisely the posture a disposable, regenerate-at-compaction cache should take, since compression ratio mostly matters for durability/transfer cost, which a disposable cache doesn't pay.
  [arXiv:2304.05028](https://arxiv.org/abs/2304.05028) · [VLDB PDF](https://www.vldb.org/pvldb/vol17/p148-zeng.pdf)

- **"BtrBlocks: Efficient Columnar Compression for Data Lakes."** Kuschewski, Sauer, Leis, Neumann. SIGMOD 2023. Composes ~8 lightweight, cascadable encodings chosen per-block by a cost-based selector, optimized for fast decompression on network/remote-storage-bound data lakes rather than maximal ratio; reported ~86 Gbps decompression throughput (near uncompressed's ~91 Gbps), 1.8–2.6x cheaper to load than Parquet+Zstd/Snappy. Reference architecture for "many small, fast, composable codecs chosen per-block," generalizing directly to CQLite's per-generation cache segments.
  [PDF](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) · [ACM DOI](https://dl.acm.org/doi/10.1145/3589263)

- **"The FastLanes Compression Layout: Decoding > 100 Billion Integers per Second with Scalar Code."** Afroozeh, Boncz. VLDB 2023, PVLDB 16(9): 2132–2144. Redesigns the physical bit-layout for lightweight schemes around a virtual 1024-bit SIMD register and fixed tuple-interleaving ("Unified Transposed Layout"), so scalar code auto-vectorizes to >40 values/cycle across Intel/AMD/Apple/AWS **without per-architecture SIMD intrinsics**. Portable, compiler-autovectorized decode speed is exactly what a Rust-native, cross-platform disposable cache wants.
  [VLDB PDF](https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf) · [ACM DOI](https://dl.acm.org/doi/10.14778/3598581.3598587)

- **"The FastLanes File Format."** Afroozeh, Boncz. VLDB 2025, PVLDB 18(11): 4629–4643. Turns the compression layout into a full format: avoids generic block compressors entirely, supports cascaded/expression encodings and **multi-column compression (MCC)** exploiting cross-column correlation, reports better compression ratio *and* faster decompression than Parquet simultaneously. MCC is a concrete idea CQLite hasn't likely considered — jointly encoding correlated columns (e.g., clustering-key-adjacent columns) rather than independently per-column.
  [VLDB PDF](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf) · [code/artifacts](https://github.com/cwida/fastlanes-vldb2025)

- **"ALP: Adaptive Lossless Floating-Point Compression."** Afroozeh, Kuffo, Boncz. SIGMOD 2024. Two-mode scheme: values that are "really" decimals get mapped to integers via power-of-10 scaling then FOR/bit-packed; true irrational/computed doubles fall back to ALP-RD (bit-splitting + dictionary). ~1–2 orders of magnitude faster (de)compression than prior float compressors with better ratio; in DuckDB, ~2-4x faster than Patas at decompression. Directly applicable to any float/double/decimal columns CQLite caches, given a strong claim of no ratio-vs-speed tradeoff.
  [SIGMOD PDF](https://ir.cwi.nl/pub/35864/35864.pdf) · [DuckDB writeup](https://duckdb.org/science/alp/) · [code](https://github.com/cwida/ALP)

- **"FSST: Fast Random Access String Compression."** Boncz, Neumann, Leis. VLDB 2020, PVLDB 13(12): 2649–2661. A static per-block symbol table sized for strings, giving compression/decompression speed comparable to or better than the fastest general compressors, significantly better ratio, and — the key property — **random access to individual compressed strings** without decompressing the whole block, enabling lazy/late decompression during predicate evaluation. The standard building block inside BtrBlocks and FastLanes for text columns; "decompress only the string you touched" maps directly onto CQLite's column-pruned, predicate-pushed-down scan model.
  [VLDB PDF](http://www.vldb.org/pvldb/vol13/p2649-boncz.pdf)

- **"Data Blocks: Hybrid OLTP and OLAP on Compressed Storage using both Vectorization and Compilation."** Lang, Mühlbauer, Funke, Boncz, Neumann, Kemper. SIGMOD 2016. HyPer's cold-data columnar format: adaptive per-block choice among ordered-dictionary compression, truncation, single-value compression, plus a fine-grained "Positional SMA" index that narrows scan ranges even when a whole block can't be pruned; execution mixes an interpreted vectorized scan (compressed cold data) feeding JIT-compiled pipelines. Direct academic ancestor of the delta-store-plus-compressed-cold-store pattern (same TUM/CWI lineage as FSST/BtrBlocks/FastLanes/ALP); the Positional SMA idea is a cheap addition CQLite could bolt onto its own cache segments.
  [ACM DOI](https://dl.acm.org/doi/10.1145/2882903.2882925) · [summary](https://sungsoo.github.io/2016/11/21/data-blocks.html)

- **Procella / Artus — "Procella: Unifying Serving and Analytical Data at YouTube."** Chattopadhyay, Dutta, et al. VLDB 2019, PVLDB 12(12): 2022–2035. Google's system unifying reporting, embedded stats, time-series monitoring, and ad-hoc analysis over one engine, serving hundreds of billions of queries/day. Introduces **Artus**, a columnar file format built to be good at *both* point lookups and full scans in the same file. Artus's dual-purpose design goal is exactly the tension CQLite may face if its cache serves point-lookup-shaped CQL queries too, not just OLAP scans; the VLDB paper doesn't fully spec Artus's layout, so treat implementation details as unverified beyond what's in the paper.
  [VLDB PDF](https://www.vldb.org/pvldb/vol12/p2022-chattopadhyay.pdf)

- **"Bullion: A Column Store for Machine Learning."** Liao, Liu, Chen, Abadi. arXiv:2404.08901 (accepted CIDR 2025). Targets ML-training-shaped columnar access: compliance-driven per-row deletes/masking without full rewrite, long sparse-sequence feature encoding, wide-table (thousands of columns) projection, in-storage feature quantization, cascading-encoding framework. Relevant if/when CQLite's cache is consumed by ML feature pipelines rather than pure SQL OLAP — otherwise secondary.
  [arXiv](https://arxiv.org/abs/2404.08901) · [CIDR 2025 PDF](https://vldb.org/cidrdb/papers/2025/p26-liao.pdf)

- **"Lance: Efficient Random Access in Columnar Storage through Adaptive Structural Encodings."** Pace, She, Xu, Jones, Lockett, Wang, Shah. arXiv:2504.15247. Argues good random access on NVMe-backed columnar storage hinges on how repetition/validity structure is encoded, not the value encoding; Lance adaptively picks per-column between scan-optimized and random-access-optimized structural encoding. Relevant if CQLite's cache ever needs to serve selective point lookups, not just full scans — orthogonal to the compression-focused papers above.
  [arXiv](https://arxiv.org/abs/2504.15247)

- **Nimble (Meta).** No peer-reviewed paper found — industry OSS project (`facebookincubator/nimble`), publicized via a 2024 internal talk, not a formal publication (**unverified as academic literature**). Meta's Parquet replacement for very-wide ML feature-store tables: Flatbuffers instead of Thrift, block encoding instead of streaming, fully independent per-column metadata. Cite as an engineering artifact, not a research paper.
  [GitHub](https://github.com/facebookincubator/nimble/)

- **Vortex (formerly SpiralDB, now LF AI & Data Incubation project).** **No dedicated academic paper found** despite direct arXiv/CIDR search — treat any claim of a "Vortex paper" as false until one surfaces. OSS project + blog-post series plus vendor-published claims ("100x faster random access, 10-20x faster scans, 5x faster writes" vs. Parquet) that are **not independently verified/peer-reviewed**. Architecturally a framework for per-column, per-block adaptive encoding selection plus a deliberately minimal footer so the "blessed" encoding set can evolve via dated editions without breaking readers — worth studying for CQLite's own cache-invalidation/regeneration story, since a disposable format expected to be rewritten often hits exactly this old-segment/newer-decoder problem.
  [project site](https://vortex.dev/) · [GitHub](https://github.com/vortex-data/vortex)

- **"Data Formats in Analytical DBMSs: Performance Trade-offs and Future Directions."** MIT; *The VLDB Journal* 2025 (arXiv:2411.14331, Nov 2024). Systematic trade-off survey of Arrow/Parquet/ORC as candidate DBMS-internal formats, concluding none is optimal for common ML workloads and arguing for a co-designed unified in-memory + on-disk representation rather than separate wire/storage formats. Closest literature-level articulation of "the existing formats have hit their design ceiling" — the premise CQLite's disposable-cache thesis implicitly bets on.
  [arXiv](https://arxiv.org/abs/2411.14331) · [VLDB Journal](https://link.springer.com/article/10.1007/s00778-025-00911-1)

- **"Should I Hide My Duck in the Lake?"** DaMoN '26, May 2026. Measures Parquet decoding consuming **~46-50% of per-query runtime** across TPC-H/ClickBench/TPC-DS against remote Parquet files, proposes a SmartNIC-based line-rate decoder + on-NIC pushdown (up to 2x TPC-H throughput). Independent, recent confirmation that decode — not I/O, not compression math — dominates the cost of scanning Parquet-shaped files, reinforcing a decode-speed-first design bet. *(Title/venue verified via search; full paper text not independently fetched — treat percentages as reported-by-secondary-source.)*
  [arXiv](https://arxiv.org/html/2602.18775v2)

- **"Do GPUs Really Need New Tabular File Formats?"** Luo, Chen, Binnig. arXiv:2602.17335 (May 2026). Argues Parquet's poor GPU-scan performance is a consequence of CPU-era default configuration (row-group size, page size, encoding choice), not an inherent format limitation — GPU-aware configuration of vanilla Parquet recovers up to 125 GB/s effective read bandwidth without a spec change. Worth stress-testing against CQLite's own numbers before committing to a bespoke format purely for decode speed — some of the wanted speedup may be achievable by tuning row-group/page sizing in a Parquet-family cache instead.
  [arXiv](https://arxiv.org/abs/2602.17335)

- **German-style / Umbra-style strings.** Origin: **"Umbra: A Disk-Based System with In-Memory Performance,"** Neumann, Freitag. CIDR 2020. The 16-byte inlined-prefix string representation (short strings fully inline; long strings as length+prefix+buffer-index+offset, enabling comparison before touching the heap) is an Umbra implementation detail popularized as "German-style strings," now adopted by DuckDB, Velox, Polars, CedarDB, and Arrow's `StringView`/`BinaryView` types. Orthogonal to block-level compression but a near-free scan-speed win for any string column in a disposable cache — worth adopting as the in-memory Arrow representation CQLite already targets, independent of the on-disk cache codec choice.
  [CIDR 2020 PDF](http://cidrdb.org/cidr2020/papers/p29-neumanncidr20.pdf) · [DataFusion blog](https://datafusion.apache.org/blog/2024/09/13/string-view-german-style-strings-part-1/) · [CedarDB explainer](https://cedardb.com/blog/german_strings/)

**Gaps / unverified**: no independent academic paper on Vortex; Nimble is documentation/OSS-only; the TiFlash DeltaTree internals came from PingCAP engineering blogs, not the VLDB paper text (a direct PDF fetch could not extract it — re-fetch and confirm before citing DeltaTree specifics anywhere load-bearing); the "Should I Hide My Duck in the Lake?" percentages are reported secondhand from search snippets, not read from the primary PDF.

### 11.6 Outside the box

This section deliberately excludes Lance, Vortex-as-primary-pitch, and Nimble except where they're the *vehicle* for a technique under review (e.g., Vortex as the reference implementation of FastLanes/late-materialization).

**1. Compute-on-compressed / late materialization.** FastLanes (the encoding engine also used inside Vortex) lays out lightweight-compressed (FOR/DELTA/RLE/DICT) data so SIMD lanes decode >100B integers/sec, and exposes fused decode+filter kernels (e.g., `FFOR`) so a predicate can prune without a full materialize pass; Vortex's DataFusion integration pushes filter expressions down into these kernels rather than decode-then-filter. A coordinator scan is already a range predicate (token range) plus usually clustering-key/column projection — exactly the shape these kernels target: for narrow token-range slices, decode work scales with selectivity rather than paying full-file decode regardless of range width. **Verdict: watch** — directly attacks C2/C5 for CQLite's access pattern, but adopting it means depending on Vortex or reimplementing FastLanes' fused kernels independently in Rust — a bigger commitment than day-one needs. Prototype once a naive Parquet/Arrow-IPC baseline exists, as the first performance escalation.
Sources: [Vortex DuckDB extension announcement](https://duckdb.org/2026/01/23/duckdb-vortex-extension) · [Vortex GitHub](https://github.com/vortex-data/vortex) · [FastLanes File Format (VLDB 2025)](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf) · [FastLanes Compression Layout (VLDB 2023)](https://dl.acm.org/doi/10.14778/3598581.3598587)

**Succinct/index-embedded structures — zone maps, PSMA, sketches.** HyPer's Data Blocks design (BtrBlocks' direct ancestor) embeds a Positional SMA (PSMA) — a tiny per-block sketch of value→position — directly inside each compressed block, narrowing the *portion of a block* to touch even when the whole block can't be pruned; one level finer than a Parquet-style per-row-group zone map. Cassandra SSTables are already sorted by (token, clustering columns) — free, authoritative sort order handed to the cache builder at conversion time (single pass, satisfies C3). A generation-keyed cache segment should embed, per block/page: min/max token + min/max clustering-key tuple (a zone map matched to the actual sort key, not a generic column stat); a small bloom/PSMA-style sketch for partition-presence/`IN`-list lookups; range-tombstone bounds as their own tiny embedded stream (per C4) so a scan can cheaply skip tombstone-merge logic. Because the cache is disposable (C8), embedding project-specific sketches tuned to Cassandra's actual sort/partition semantics — rather than adopting a generic external index format — is low-risk, high-leverage: no long-term index-format lock-in, and CQLite already knows the exact clustering order at write time. **Verdict: adopt now (as design principle, not a specific library)** — cheap to build on top of whatever base container format is chosen, no external dependency, and the highest-leverage, lowest-risk item in this survey.
Sources: [BtrBlocks paper](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) · [CMU 15-721 notes on Data Blocks/PSMA](https://15721.courses.cs.cmu.edu/spring2024/notes/03-data2.pdf) · [Succinct data structures overview](https://en.wikipedia.org/wiki/Succinct_data_structure) · [`vortex-btrblocks` Rust crate](https://crates.io/crates/vortex-btrblocks/0.30.0) · [original BtrBlocks (C++) repo](https://github.com/maxi-k/btrblocks)

**2. GPU-native layouts.** cuDF/libcudf reads Parquet directly on the GPU via KvikIO + GPUDirect Storage; a widely-cited 2026 finding puts ~85% of TPC-H runtime in such systems in the Parquet *scan* step, meaning format GPU-friendliness dominates GPU query speed. The decisive counter-finding — a Feb 2026 paper "Do GPUs Really Need New Tabular File Formats?" — concludes **no**: GPU-tuned settings on ordinary Parquet (larger pages, multi-million-row row groups, per-chunk flexible encoding, skipping marginal-gain compression) raise effective GPU scan bandwidth to ~125 GB/s — the win is in *configuration*, not a new byte layout. **Verdict: ignore (as a distinct format), watch (as a tuning knob)** — no CQLite consumer is GPU-resident today, and the paper removes the incentive to invent a new byte layout for this reason; revisit only if/when a GPU-backed analytics tier is actually built.
Sources: [Do GPUs Really Need New Tabular File Formats?](https://arxiv.org/html/2602.17335v1) · [NVIDIA GPUDirect Storage + RAPIDS cuDF](https://developer.nvidia.com/blog/boosting-data-ingest-throughput-with-gpudirect-storage-and-rapids-cudf/) · [cuDF docs](https://docs.rapids.ai/api/cudf/stable/) · [Vortex GPU paper (VLDB 2025)](https://web.eecs.umich.edu/~linmacse/publications/2025.vortex.vldb.pdf) · [GPU Acceleration of SQL Analytics on Compressed Data](https://arxiv.org/abs/2506.10092) · [G-ALP (CWI)](https://ir.cwi.nl/pub/35205/35205.pdf)

**4. Learned/semantic compression (LeCo).** LeCo (SIGMOD 2024) generalizes FOR/Delta/RLE by fitting a learned model of a column's serial correlation (e.g., linear/piecewise over sorted/near-sorted integers) and storing residuals; reported Pareto-improvement in both ratio and random-access decode speed vs. existing lightweight schemes, up to 5.2x on filter-groupby-aggregation. Cassandra clustering columns, timestamps (writetime/TTL — directly named in C4), and token values are exactly the near-monotonic sequences LeCo targets — a strong conceptual fit for C4's writetime/TTL streams. **Maturity check**: research-only — the reference implementation ([yhliu918/Learn-to-Compress](https://github.com/yhliu918/Learn-to-Compress)) is an academic C++ prototype, no Rust port, no crate, no production adoption signal; fails both C6 and C7 today. **Verdict: watch** — right idea for the writetime/TTL columns, but no production-grade Rust implementation exists; keep as inspiration for an in-house delta model rather than a dependency.
Sources: [LeCo (SIGMOD '24)](https://dl.acm.org/doi/10.1145/3639320) · [arXiv version](https://arxiv.org/abs/2306.15374) · [reference implementation](https://github.com/yhliu918/Learn-to-Compress)

**5. Object-storage/range-request-native design.** Footer-first layouts (Parquet's own model) let a logical read become N targeted HTTP Range GETs instead of a full-object fetch; AnyBlob (VLDB '23) is a cloud-agnostic, io_uring-based download manager minimizing per-request CPU overhead across S3/GCS/Azure. Not relevant to the hot local-NVMe tier (mmap wins there), but directly relevant if the disposable cache ever tiers to object storage — offloading cold-generation segments to S3, or a future distributed-coordinator design sharing cache segments via blob storage. Because the C2 fine-grained-seek requirement already forces a footer-first, block-addressable layout, this is essentially free if C2 is done right. **Verdict: watch (design-compatible by default, not an active build item)** — no current requirement tiers the cache to object storage, but it becomes an option later without redesign.
Sources: [AnyBlob (VLDB '23)](https://www.durner.dev/app/media/papers/anyblob-vldb23.pdf) · [AWS: range/partNumber headers for OLAP-style S3 reads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/range-get-olap.html) · [F3 project](https://github.com/future-file-format/F3)

**6. Memory-format-as-disk-format: "just Arrow IPC + a tiny sidecar index."** Arrow's own maintainers explicitly do **not** recommend IPC as an at-rest format, since it skips the block-level compression/lightweight encoding Parquet applies, making files "often much" larger (no authoritative single multiplier found; commonly cited informally as 2-5x for mixed-type data — **unverified, anecdotal**, and pathological cases like long UTF-8/sparse columns can be worse). The trade is I/O bytes moved vs. CPU cycles decoding: on saturated/network-attached storage or with real compression ratios ≥3-4x, Parquet's smaller byte count wins because I/O is the bottleneck. But for a **local, single-node, hot-tier cache on NVMe** — exactly C1's target — decode-free mmap access has zero CPU cost and zero extra memory copy; if the size penalty stays within a small integer multiple of the compressed size, the eliminated decode step plus true zero-copy Arrow handoff can outweigh the extra bytes moved, especially once the OS page cache amortizes the first-touch I/O cost. This is uniquely attractive for a **disposable** cache in a way it would never be for a permanent format: the "penalty" is a local-disk space cost regenerated at every compaction anyway, not a long-term footprint commitment. The §"zone maps/PSMA" sidecar index recovers most of C2's fine-grained-seek requirement that raw Arrow IPC lacks natively (batch-level only addressability). **Verdict: watch, with a fast experiment recommended** — the single most CQLite-native idea in this survey (mmap + Arrow IPC + a purpose-built sidecar index, all in Rust, no new format dependency), but the actual size/latency trade needs measurement on real cache segment shapes (wide sparse UDT columns, per-cell writetime/TTL streams) before committing — the generic "Arrow IPC is bigger" folklore doesn't account for CQLite's specific envelope shape.
Sources: [Apache Arrow FAQ — IPC vs Parquet trade-offs](https://arrow.apache.org/faq/) · [Comparing Data Storage: Parquet vs Arrow (anecdotal)](https://medium.com/@diehardankush/comparing-data-storage-parquet-vs-arrow-aa2231e51c8a) · [Data Formats in Analytical DBMSs: Performance Trade-offs](https://arxiv.org/pdf/2411.14331)

**7. Genuinely novel 2024-2026, beyond Lance/Vortex/Nimble.** **F3 ("Future File Format," SIGMOD 2026, CMU DB Group)**: each file self-describes down to its codec, embedding **WebAssembly binaries of its own decoders** alongside data + metadata, so any reader can decode any encoding scheme without native support — attacking the "new encodings require new reader code" lock-in problem every columnar format eventually hits. A Rust implementation exists (`fff-poc`, `fff-bench`) but the authors flag it explicitly as a research prototype not for production use. Philosophically the same instinct as "cache is disposable, so lock-in is near zero" — except F3 solves it by embedding portable decoders in each file, while CQLite's answer is simpler: regenerate the whole cache segment in a new format when needed. F3 solves a harder, more general problem (permanent-storage format evolution) that CQLite's disposability assumption sidesteps entirely. **Verdict: ignore for adoption, watch for the idea** — pre-production research code, and CQLite doesn't need the self-describing-codec mechanism its disposability already grants for free. **FastLanes File Format (VLDB 2025)** — already covered above, but also flaggable separately as itself a genuinely new (2025) proposed on-disk format. **Verdict: watch**, same reasoning as the compute-on-compressed entry. **G-ALP / ZipFlow (2025-2026)** — narrow GPU-kernel-level research re-deriving FOR/RLE encodings for GPU throughput. **Verdict: ignore for now** — same rationale as the GPU-native-layouts entry, no GPU-resident consumer exists in CQLite's architecture today.
Sources: [F3 GitHub](https://github.com/future-file-format/F3) · [F3 paper](https://db.cs.cmu.edu/papers/2025/zeng-sigmod2025.pdf) · [CMU Future File Formats project page](https://db.cs.cmu.edu/projects/future-file-formats/) · [FastLanes File Format (VLDB 2025)](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf) · [ZipFlow](https://arxiv.org/html/2602.08190) · [G-ALP (CWI)](https://ir.cwi.nl/pub/35205/35205.pdf)

**Summary verdict table**

| Direction | Verdict | One-line why |
|---|---|---|
| Compute-on-compressed (FastLanes/Vortex kernels) | **watch** | Best C2/C5 fit for token-range scans, but a real dependency commitment — second-wave optimization, not v1 |
| Embedded zone maps/PSMA on clustering order | **adopt now (as design principle)** | Free given CQLite already knows sort order at write time; highest leverage, lowest risk item here |
| GPU-native layouts | **ignore (format), watch (config)** | 2026 paper shows format redesign isn't needed; no GPU consumer exists yet anyway |
| Learned compression (LeCo) | **watch** | Right idea for writetime/TTL columns (C4), but zero production-grade Rust implementation |
| Object-storage/range-request-native | **watch (comes free)** | Not needed today; falls out automatically if the C2 block-addressable design is done right |
| Arrow-IPC-only + sidecar index | **watch, prototype soon** | Most CQLite-native option surveyed — needs a real size/latency measurement before committing |
| F3 (self-describing Wasm-codec format) | **ignore (adopt), watch (idea)** | Pre-production research code; solves a lock-in problem CQLite's disposability already sidesteps |
| FastLanes-the-format | **watch** | Same reasoning as compute-on-compressed |
| G-ALP/ZipFlow (GPU encoding research) | **ignore** | No GPU-resident consumer in CQLite's architecture |

**Unverified/anecdotal flags carried forward**: the "2-5x size penalty" figure for Arrow IPC vs Parquet is community folklore, not a benchmarked number from a controlled study — needs its own measurement on CQLite's actual cache envelope. Vortex's "100x faster random access" and "10-20x faster scans" claims are vendor-reported, not independently reproduced.

### 11.7 Recommendation ladder

The evidence assembled here does not point to a single obvious winner — it points to a **staged bet**, and it's worth being explicit about where the confidence is real versus where it's still a hypothesis.

**Hot tier (today, v1): Parquet via arrow-rs, with a CQLite-authored zone-map/PSMA sidecar keyed to (token, clustering).**
- Justification: C6/C7 — the only candidate with a mature, production-proven, officially-maintained Rust implementation and Apache-2.0 governance today, not a roadmap promise. C1 — zero-copy Arrow via arrow-rs is real and shipping. C2 — Parquet's native page-level ColumnIndex/OffsetIndex already gives page-granularity pruning, and stacking a Cassandra-sort-order-aware zone map/PSMA on top (§11.6, "adopt now") closes most of the remaining gap to a token-range-native seek without adopting a new format. C8 — usable today is not in question.
- Explicit tradeoff accepted: Parquet's Thrift footer overhead on wide/sparse schemas (C4) is real and unresolved by this choice alone; it is accepted because rebuild cost (not per-segment bytes or decode-constant-factor) is the primary cost function for a disposable cache, and Parquet is the safe default while the harder bet below is benchmarked.

**Escalation candidate (v1.x, pending a bench workstream): Vortex.**
- Justification: on paper, Vortex is the single best fit across C1 (true zero-copy Arrow, mmap-by-design footer), C2 (chunk/zone-level pruning below row-group granularity, StructLayout per-field addressing — directly relevant to C4's writetime/TTL sidecar columns), C5 (compute-on-compressed, decode-speed-first design matching the disposable-cache thesis), and C3 (bounded-memory streaming writer, single-pass-compatible with converting an immutable `nb` SSTable). C6/C7 have real signal — active Rust crates, Apache-2.0, now under LF AI & Data Incubation governance, not a single-vendor repo.
- What holds this back from being the v1 pick: it is genuinely pre-1.0 (its own docs say "Library APIs may change from version to version"), its headline performance numbers (100-200x random access, 2-10x scans vs. Parquet) are vendor-reported and not independently reproduced in any source found here, and named production users beyond the vendor's own ecosystem are thin. Per C8's framing, this instability is a **weak** negative given disposability — it does not disqualify Vortex, it just means it hasn't yet earned the "default" slot the way Parquet has.

**Cold tier (if/when it exists): Parquet, optionally under an Iceberg-style manifest, deliberately boring.**
- Justification: this is the one place in the whole survey where C8's "stability is a weak negative" reasoning explicitly reverses (§11.1) — a cold tier shared with external lakehouse consumers (Spark/Trino/DuckDB, or a durable export) is a contract with someone else's engine, not a disposable artifact CQLite alone controls. Iceberg's manifest-as-stats-table pattern (§11.3) is worth adopting at a much-reduced scale (a tiny generation-keyed manifest) without the full snapshot/ACID machinery none of which CQLite's immutable-per-generation model needs.

**Watch list, not to be built yet:**
- FastLanes-the-full-format and Vortex's underlying compute-on-compressed kernels (C2/C5 upside, real dependency cost) — §11.6 direction 1.
- LeCo-style learned compression for writetime/TTL columns (C4 fit, zero Rust implementation) — §11.6 direction 4.
- Object-storage range-request-native design — comes for free if the zone-map/C2 design is done right; no action needed until a cold/shared tier is real — §11.6 direction 5.
- F3's self-describing Wasm-codec mechanism — solves a lock-in problem CQLite's disposability already sidesteps; explicitly pre-production — §11.6 direction 7.
- Nimble's cascading-encoding idea and BtrBlocks' per-block adaptive scheme selection — conceptually reusable, but both are C++-only with no Rust path (C6 fails outright); worth stealing the *idea*, not the crate.

**What evidence is missing (the bench workstream this section should trigger before any of the above is locked in):**
1. No benchmark exists in this survey of any candidate against **real `nb`-converted cache segments** — every number cited (BtrBlocks' 2.2x, Vortex's 100-200x, Lance's ~1-2 IOPS, ALP's 2-4x) is from the candidate's own paper/blog on its own benchmark data, not CQLite's actual envelope shape (wide sparse UDT columns, per-cell writetime/TTL i64 streams, a separate range-tombstone stream). This is the single most important gap — §11.6 direction 6 explicitly calls for measuring the Arrow-IPC-plus-sidecar approach against Parquet on real converted segments before committing to either.
2. No measurement of **token-range seek latency** under CQLite's own zone-map/PSMA sidecar design (proposed, not yet built) against Parquet's native page index, Lance's IOPS-based random access, or Vortex's chunk pruning — the C2 comparisons in this report are architectural (what the format's layout permits), not empirical (what CQLite's implementation actually achieves).
3. No data on **decode-speed vs. compression-ratio tradeoff at CQLite's specific rebuild cadence** (per-compaction) — the "decode speed >> ratio" thesis (C5) is well-supported in the literature generally, but the actual crossover point (at what compression ratio does the extra I/O outweigh the saved CPU, on CQLite's target hardware) has not been measured.
4. No confirmation of **Vortex's write-path memory bound** on CQLite's actual conversion path (single-pass from an immutable `nb` SSTable) — the streaming-writer claim is architectural, inferred from the project's own description, not independently verified against a workload resembling CQLite's.
5. No comparison of **Parquet's Thrift-footer overhead vs. Vortex's minimal-footer/FlatBuffer approach** at CQLite's actual schema widths (dozens, not Nimble's 10k+, columns) — the wide-schema case for Vortex/Lance is argued from ML-feature-table-scale examples in their own literature, and it's unverified whether the benefit shows up at CQL-table scale.

Where profiles in this survey conflicted or left claims unverified, this section has flagged them inline rather than resolving them by fiat (e.g., BtrBlocks' Rust-availability status differs slightly between the format-profile write-up, which found none, and the outside-the-box survey, which notes a `vortex-btrblocks` crate exists but is not a reference-implementation port). The recommendation above is a bet informed by architecture and governance signal, not by CQLite-specific measurement — that measurement is the next required step, not an optional follow-up.

---

## 12. Latency-tree paper simulations

### 12.1 Method and constants

These are **paper simulations**, not benchmark measurements. Each tree walks a query's critical path top-to-bottom on a single replica; every leaf line shows the formula that produced it (`bytes / rate = time`, or `rows × per-row cost = time`) tagged with the constant ID it draws on, so any reader can recompute it by swapping in a different number. Stages are summed sequentially except where a tree explicitly marks two branches as concurrent (`per-node critical path = max(A, B)`) — this happens only in scenarios where tail acquisition and bulk decode are architecturally independent (S3). The cluster envelope is always `coordinator parse/plan + fan-out RPC + max(per-node latency across the 6 fanned-out ranges) + network return/aggregate`, per the fixed fan-out=6, RF=3 parameter. A `[ASSUMED]` tag marks a value with no backing in either constants table; a bracketed `[Q-*]`/`[C-*]` tag cites the constants-table row a number is drawn from, however loosely. Where the verifier corrected a number below, the corrected value is used and flagged **(corrected in verification)**; the full corrections list is reproduced verbatim in §12.6.

**Merged constants table** (repo-measured rows first, then literature-sourced):

| ID | Constant | Value | Source | Confidence |
|---|---|---|---|---|
| Q-VALUE-SIZE | `size_of::<Value>()` (row-cell in-memory representation) | 88 bytes (pinned ceiling) | `cqlite-core/src/types.rs:91`; `read-path-performance-audit-2026-07-01.md:19,111,162` | measured (compile-time assertion) |
| Q-VALUE-SHRUNK | Projected `size_of::<Value>()` post Epic E1 | ≤ 40 bytes (target, unlanded) | `read-path-performance-audit-2026-07-01.md:57,111`; `cqlite-core/src/types.rs:88-90` | derived (unimplemented) |
| Q-CHUNK-WAKE-42 | Read wall-time parked on raw-chunk channel futex, pre-fix | ~42% | `.../scan_stream_windowed.rs:144-146` (issue #1143, samply profile) | measured |
| Q-ROW-WAKE-31 | Read wall-time on per-row condvar wake, pre-batching | ~31.5% | `.../scan_stream_windowed.rs:166-169` (issue #1143, samply profile) | measured |
| Q-WAKE-AMORTIZE-DEPTH | Wake amortization, channel cap 2→8 | ~4× | `.../scan_stream_windowed.rs:149` | derived (not re-measured) |
| Q-WAKE-AMORTIZE-BATCH | Wake amortization from `BATCH_EMIT_ROWS` | ~256× | `.../scan_stream_windowed.rs:172-173,183` | derived (design constant) |
| Q-CHUNK-SIZE | SSTable compression chunk length | 16–64 KiB | `.../scan_stream_windowed.rs:151` | measured |
| Q-SCAN-LAT-BUF-N1 | Full-scan median latency, buffered, N=1 | 7.93 ms | `docs/profiling.md:97` (Criterion, dev machine) | measured |
| Q-SCAN-LAT-MMAP-N1 | Full-scan median latency, mmap, N=1 | 4.62 ms | `docs/profiling.md:97` | measured |
| Q-SCAN-LAT-BUF-N8 | Full-scan aggregate median, buffered, N=8 | 31.81 ms (1.99× scaling) | `docs/profiling.md:100` | measured |
| Q-SCAN-LAT-MMAP-N8 | Full-scan aggregate median, mmap, N=8 | 7.69 ms (4.81× scaling) | `docs/profiling.md:100` | measured |
| Q-SCAN-ROWS-DERIVED-N1-BUF | Implied throughput, buffered, N=1 | ≈126,000 rows/s | derived, `docs/profiling.md:97` × `validation-matrix.md:109` | derived |
| Q-SCAN-ROWS-DERIVED-N1-MMAP | Implied throughput, mmap, N=1 | ≈216,000 rows/s | derived, same basis | derived |
| Q-PY-EXEC-ROWS | Python `execute()` throughput | 16,317 rows/s | `validation-matrix.md:581,600` | measured |
| Q-PY-STREAM-ROWS | Python streaming throughput | 54,242 rows/s | `validation-matrix.md:582` | measured |
| Q-PY-FIRST-ROW | Python time-to-first-row | 33.16 ms | `validation-matrix.md:583` | measured |
| Q-PY-STREAM-MEM | Python streaming peak memory | 0.03 MB | `validation-matrix.md:580` | measured |
| Q-WAL-OFF-SPEEDUP | `ingest_wal_off` vs `ingest_wal_on` speedup | ~430× | `docs/performance.md:252-253` | measured (write-path) |
| Q-WAL-ON-OPSSEC | `ingest_wal_on` throughput (SyncEachWrite) | ~282 ops/sec | `docs/performance.md:5,229-245` | measured (write-path) |
| C-NVME-SEQ | NVMe sequential read, PCIe4 | 5.0–7.5 GB/s | Samsung 990 Pro / Micron 7450 datasheets | sourced |
| C-NVME-LAT | NVMe random 4K read latency, QD1 | 20–100 µs | simplyblock; Micron 7450 spec; Tom's Hardware | sourced |
| C-PGCACHE | Page-cache/memory sequential bandwidth | 6–10 GB/s (single-thread); ≤64–70+ GB/s (DDR5 ceiling) | James Slocum; Handmade Network; DDR5 SDRAM wiki | sourced/estimated |
| C-MMAP-FAULT | mmap minor-fault overhead | avg 1.27 µs, tail 3.20 µs | ACM TACO 2022; NP-RDMA arXiv | sourced |
| C-UDS | UDS/shared-memory loopback | ~4.5 µs RTT; ~multi-GB/s >1KB payloads | Linux IPC Shootout; rigtorp/ipc-bench; Myhro | sourced/estimated |
| C-GRPC | gRPC/Arrow Flight overhead + throughput | ~1–2 ms/call; up to 6000/4800 MB/s local; ~2.5–3.0 GB/s on 25GbE | ACM 2022 Flight benchmark; Nexthink | sourced/estimated |
| C-NET-25G | 25GbE throughput + intra-DC RTT | ~2.8–3.0 GB/s; RTT 5–10 µs (rack) / 50–200 µs (app) | iperf3 guide; PL2 arXiv; TIMELY SIGCOMM | sourced/estimated |
| C-PARQ-DEC | Parquet decode throughput/core | ~0.15–1 GB/s/core | VLDB 2023 empirical formats; FastLanes VLDB 2025; BtrBlocks | estimated |
| C-ARROW-IPC | Arrow IPC mmap scan rate | ≈ memory-bandwidth-bound (≈C-PGCACHE) | Arrow C++ docs; DataFusion #15321 | estimated |
| C-VORTEX | Vortex vs Parquet multipliers | ~100–200× random access, 2–10× scans, 5× writes | vortex-data/vortex; Dremio; Data Eng Central | vendor-reported |
| C-CASS-FLUSH | Cassandra memtable flush throughput | 100+ MB/s (HDD) to several GB/s (SSD) | abiasforaction.net; Cassandra docs; Instaclustr | estimated |
| C-CASS-ACK | Cassandra QUORUM ack latency, intra-DC | single-to-low-double-digit ms | ScyllaDB benchmark; ScyllaDB glossary | estimated |
| C-CASS-REPL | Replica propagation lag, intra-DC | single-digit–tens of ms | DataStax hinted-handoff docs; Medium | estimated |
| C-TRIE-ITER | Trie/skiplist iteration rate | ~10M–100M rows/s (est.) | VLDB 2022 Trie Memtables (Lambov) | estimated, low-confidence |
| C-JVM-SER | JVM serialization throughput | 100s of MB/s (order of magnitude) | jvm-serializers wiki; Kryo v1 benchmarks | estimated |
| C-FSYNC | fsync + atomic-rename latency | 0.14 ms (enterprise NVMe) to 3.8–9.6 ms (consumer SSD) | Percona; evanjones.ca | sourced |

**Missing measurements** (no value exists anywhere in the repo; carried forward to WS7):
- nb Data.db sequential decode throughput, directly reported (no committed Criterion output for `read/full_scan`/`m1_performance`; only a `>100MB/s` target).
- Per-value/per-cell decode overhead (ns/value); proposed bench (`type_heavy`, dhat lane) not yet run.
- k-way merge per-row cost (ns/row or rows/s) — explicit "blind spot today" (Epic O, write-path audit).
- Flight/Arrow emission throughput — no export-path audit file exists in this checkout.
- Parquet export throughput — same absence.
- Node.js binding overhead/throughput — no bindings-FFI audit file exists; no number cited anywhere.
- Compression/decompression rates (LZ4/Snappy/Zstd/Deflate) — harness exists but is unwired/dead; no committed result.
- BTI trie descent cost (ns/node, nodes/sec) — only complexity assertions proposed, not timings.
- Tail latency (p50/p99/p999) — harness exists, every gated bench reports median only.
- `history.jsonl` ledger contents — gitignored, generated-only, empty in this checkout.

Two requested source files do not exist in this repo: `docs/reports/export-path-performance-audit-2026-07-01.md` and `docs/reports/bindings-ffi-performance-audit-2026-07-01.md` (confirmed via full-repo search). Only the read-path, write-path, and parser audits are present.

### 12.2 The scenarios

- **S0** — Snapshot → Sidecar → Spark bulk reader (CEP-28/Cassandra Analytics prior art): operator-triggered snapshot, full-component HTTP streaming to Spark, no pushdown, no tail.
- **S1** — Status-quo CQLite: standalone external Flight server, per-query `nb` decode of flushed data only, no tail crossing at all (unflushed writes invisible).
- **S2** — v1 coordinator-native: on-demand JVM-side tail-snapshot export (dirty-check gated, 5s min interval) plus per-query `nb` bulk scan, no columnar cache.
- **S3** — v2 coordinator-native: live-stream tail (readOrdering pin, nb-serialized over UDS) merged against per-query `nb` bulk decode, no snapshot file, no cache.
- **S4A** — v2 + hot Arrow IPC mmap cache (warm): pre-materialized columnar Arrow segments per generation, zero-copy warm reads, live tail still crosses as raw `nb`.
- **S4B** — v2 + Parquet+zone-map cache (warm), the section-11.7 v1 pick: compressed columnar shadow files with CPU-bound decode instead of mmap's zero-copy read; tail unchanged from S4A.
- **S5** — Parquet-attached SSTables (dual-format compaction): every flush/compaction writes both `nb` and Parquet; no tail path exists at all (writes invisible until flush).
- **S6** — Continuous columnar replica (TiFlash-style delta+stable), CDC-shaped, presented only as a contrast: every write is applied twice via a mutation feed the owner has ruled out, eliminating per-query JVM↔engine crossing entirely.

### 12.3 Latency trees

#### S0 — Snapshot → Sidecar → Spark

**Freshness:** ~30 min avg / ~60 min worst-case, cadence-dominated (pipeline run of 31–195s is noise by comparison) — hourly snapshot cadence [ASSUMED].

```
W1 — interactive slice (100MB flushed + 128MB tail/node, 4/20 cols nominal; agg pushdown live-design-only)

operator/scheduler triggers pipeline
└─ snapshot creation (per node, parallel across 6 replicas)
   ├─ forced flush of 128MB tail: 20.8ms seq write [C-NVME-SEQ, ASSUMED write≈read] + 10ms fsync×2 [C-FSYNC] = 30.8 ms
   └─ hardlink 10 files [ASSUMED] × ~1ms/file = 10 ms
   subtotal ............................................. 41 ms
└─ Spark job submission + DAG plan + executor allocation .. 30,000 ms  [ASSUMED midpoint of "tens of seconds"]
└─ per-node (fan-out, 1 task/range):
   ├─ Sidecar egress, FULL component set: 100MB/2800MB/s [C-NET-25G] = 35.7 ms
   └─ JVM nb decode (no projection) + merge: 100MB/800MB/s [ASSUMED] = 125 ms
└─ Spark shuffle + driver aggregate ........................ 500 ms [ASSUMED]
TOTAL .................................................... ~30,702 ms (~30.7s)  dominant: job startup (97.7%)
```

```
W2 — table scan (10GB flushed + tail, 8/20 cols)
snapshot (81ms) + job startup (30,000ms) + egress 10,240MB/2800MB/s=3,657ms + decode 10,240MB/800MB/s=12,800ms + shuffle 500ms
TOTAL .................................................... ~47,038 ms (~47.0s)  dominant: job startup (63.8%), decode (27.2%)
```

```
W3 — heavy (100GB flushed + tail, 8/20 cols)
snapshot (231ms) + job startup (30,000ms) + egress 102,400MB/2800MB/s=36,571ms + decode 102,400MB/800MB/s=128,000ms + shuffle 500ms
TOTAL .................................................... ~195,302 ms (~3m15s)  dominant: decode (65.5%), egress (18.7%), startup now minor (15.4%)
```

**Standing costs:** write amplification (24×128MB=~3GB/day extra flush at hourly cadence, ~30GB/day extra compaction I/O at ~10× write-amp [ASSUMED]); storage bloat (hardlinks pin the whole snapshot generation until TTL clear); idle cost ≈0 between runs (S0's structural strength); a wholly separate always-warm Spark cluster sized for W3 (600GB aggregate/cluster); no incremental mode — every run re-transfers and re-decodes from scratch.

**Dominant-term analysis:** at W1, job startup (30s flat) swamps everything (97.7%) — a 100MB slice pays the same tax as a 100GB scan; by W3, full-width JVM decode (no pushdown) dominates (65.5%). Cost is invariant to query selectivity: 8-of-20-column filtered work costs what `SELECT *` costs.

#### S1 — Status-quo CQLite, external Flight, no tail

**Freshness:** `T_flush_trigger + T_flush_write` — write-rate dependent and unbounded by CQLite; illustrative ~10 min for a moderately-written 128MB tail, hours for a lightly-written one [ASSUMED, no fixed number in either doc set].

```
W1 — interactive slice (100MB flushed, 4/20 cols, agg pushed down)
parse/plan 0.5ms + fan-out 0.1-0.2ms
per-node: gen-pin 0.1ms + tail=0ms (no tail path) +
   bulk scan: decode 100MB/100MB/s[ASSUMED D_decode] = 1000 ms (io 16.7ms[C-NVME-SEQ] hidden) +
   merge: 500,000 rows × 50ns[ASSUMED, per C-TRIE-ITER] = 25 ms + emit 0.3ms
+ return (colocated 0.5ms [C-UDS] / remote 1.5ms [C-GRPC,C-NET-25G])
TOTAL (colocated) ~1027 ms | TOTAL (remote) ~1028 ms   dominant: nb decode (~97.4%)
```

```
W2 — table scan (10GB flushed, 8/20 cols)
per-node: decode 10,000MB/100MB/s = 100,000 ms + merge 50,000,000×50ns = 2,500 ms + emit 1ms
TOTAL (colocated) ~102.5 s | TOTAL (remote) ~102.5 s   dominant: nb decode (~97.6%)
```

```
W3 — heavy (100GB flushed, 8/20 cols)
per-node: decode 100,000MB/100MB/s = 1,000,000 ms + merge 500,000,000×50ns = 25,000 ms + emit 2ms
TOTAL (colocated) ~1025.0 s ≈ 17.1 min | TOTAL (remote) ~1025.0 s ≈ 17.1 min   dominant: nb decode (~97.6%)
```

**Assumptions:** `D_decode ≈ 100 MB/s` is the stated design *target*, not measured (`m1_performance.rs:234-282`); avg row width ≈ 200 B/row [ASSUMED]; merge cost ≈ 50 ns/row, order-of-magnitude anchored to [C-TRIE-ITER] (no measured merge bench exists); Arrow emit modeled as a small fixed constant since results are small relative to scanned bytes; fixed control-plane constants (parse/plan 0.5ms, gen-pin 0.1ms, RPC 0.1–0.2ms, aggregate 0.2ms) are illustrative, not sourced; decode modeled single-threaded per query (10-core node gives query *throughput*, not single-query speedup, consistent with [Q-SCAN-LAT-BUF-N8]/[Q-SCAN-LAT-MMAP-N8]'s sub-linear 1.99×–4.81× scaling); disk read is 1.67% of decode time at every scale per [C-NVME-SEQ] (bytes cancel in the ratio), so bulk scan ≈ decode throughout. *Flagged, not applied:* S1's 100 MB/s buffered pick diverges from S2's 172 MB/s mmap pick for the identical no-cache decode architecture — an unreconciled ~40% swing (see §12.6, Correction #5); S1's headline totals are kept as published per the verifier's verdict, which permits either reading.

**Standing costs:** zero write amplification, zero background CPU (writes flow through Cassandra's normal path unchanged, [C-CASS-FLUSH]); no persistent cache, so no rebuild cost and no reserved cache memory — any OS page-cache residency is opportunistic; the standing costs are operational (a second always-alive Flight-server process per node) and correctness-adjacent (unbounded read-after-write staleness paid continuously, even with zero query traffic).

**Dominant-term analysis:** at every scale, `nb` decode is ~97.3–97.6% of per-node latency (merge a distant ~2.4–2.5%) — scale-invariant because both terms are linear in bytes-touched. Disk I/O is hidden because assumed decode throughput (100 MB/s) is ~60–75× slower than NVMe sequential bandwidth. **(corrected in verification)** A 10× decode-rate improvement (100→1000 MB/s) does **not** put W1 "in interactive territory at ~30 ms" as originally claimed: `100MB/1000MB/s = 100 ms`, still ~4× merge's 25 ms — decode remains dominant (~79% of total) and the corrected total is **~127 ms**, not ~30 ms. Reaching ~30 ms requires roughly a **40–50×** decode improvement (≥4,000–20,000 MB/s), not 10×.

#### S2 — v1 coordinator-native, on-demand tail export, no cache

**Freshness:** worst case `min_interval_cap + export_time = 5000ms + 43ms ≈ 5.04s`; best case ≈ 43 ms.

```
W1 — interactive slice (100MB flushed + 128MB tail, 4/20 cols projected)
parse/plan 0.5ms + fan-out 0.3ms
per-node: gen-pin 0.1ms +
   tail acquisition: cold 43.0ms [128MB/3000MB/s C-CASS-FLUSH-adjacent + 0.28ms C-FSYNC] / warm 0ms +
   bulk scan (mmap): 100MB/172MB/s[derived, Q-SCAN-LAT-BUF-N1/MMAP-N1] = 581.4 ms +
   tail scan (every query, no cache): 128MB/172MB/s = 744.2 ms +
   merge: (500,000+640,000) rows × 300ns[ASSUMED] = 342.0 ms + emit 66.9ms[Q-VALUE-SIZE proxy]
+ return 1.7ms [C-GRPC]
TOTAL (cold) ~1,780.1 ms | TOTAL (warm) ~1,737.1 ms   dominant: tail scan, 744ms — fixed tail EXCEEDS the 100MB bulk cost
```

```
W2 — table scan (10GB flushed + tail, 8/20 cols)
bulk scan 10,000MB/172MB/s = 58,140.0ms + tail scan 744.2ms + merge 50,640,000×300ns=15,192.0ms + emit 5,942.0ms
TOTAL (cold) ~80,063.8 ms (~80.1s) | TOTAL (warm) ~80,020.8 ms   dominant: bulk scan (72.6%); tail now rounding error
```

```
W3 — heavy (100GB flushed + tail, 8/20 cols)
bulk scan 100,000MB/172MB/s = 581,400.0ms + tail scan 744.2ms + merge 500,640,000×300ns=150,192.0ms + emit 58,742.0ms
TOTAL (cold) ~791,123.8 ms (~13.19 min) | TOTAL (warm) ~791,080.8 ms   dominant: bulk scan (73.5%); merge #2 (19.0%); emit now visible (7.4%)
```

**Assumptions:** export rate ≈3,000 MB/s [C-CASS-FLUSH, ASSUMED point pick]; fsync+rename ≈0.28ms [C-FSYNC]; buffered nb decode ≈100 MB/s (stated target, not measured); mmap ≈172 MB/s = 100×(7.93/4.62) [Q-SCAN-LAT-BUF-N1/MMAP-N1 ratio, derived]; row width ≈200 B/row [ASSUMED]; merge ≈300 ns/row [ASSUMED, flagged blind spot]; emit throughput ≈6,000 MB/s [C-PGCACHE low end]; emit per-cell proxy = 88B ceiling [Q-VALUE-SIZE, overstates cost]; control-plane constants generic [ASSUMED/C-GRPC]; no intra-query multi-core decode modeled (conservative upper bound, consistent with the single-stream windowed-scan architecture); tail scanned in full every query regardless of range restriction (it's one small fresh SSTable, not range-sliced).

**Standing costs:** no background daemon, no proactive rebuild, idle CPU ≈0 (S2's structural strength) — but ~128MB lingering tail-snapshot footprint; under bursty queries faster than the 5s cap, sustained re-export write load ≈128MB/5s≈25.6 MB/s/node, purely re-serializing largely-unchanged bytes; effectively zero incremental memory (no cache retained between queries — the flip side of paying full decode every time).

**Dominant-term analysis:** at W1 the fixed 128MB tail decode (744ms) exceeds the 100MB bulk-scan cost (581ms) and dwarfs the export step (43ms) — the "no cache" tail re-decode, not the export mechanism, is the real interactive-latency problem. At W2/W3 bulk-scan dominates (73–74%), merge is a real #2 (19–20%), emit becomes visible only at W3 (~7%). The parameter that flips every number is decode throughput; raising `min_interval_cap` past 5s only amortizes the 43ms export and does nothing for decode cost — a small win ceiling in this no-cache design.

#### S3 — v2 coordinator-native, live-stream tail, no cache

**Freshness:** ~0 ms on the acking replica (write lands directly in the trie memtable the next query's pin iterates); bounded by `≤` replica convergence lag on a stale CL.ONE replica: single-digit–tens of ms [C-CASS-REPL].

> **(corrected in verification)** The published trees for S3 contained two compounding errors (verifier Correction #2): bulk `nb` decode was modeled as *speeding up* with fewer projected columns — contradicting S2's own explicit doctrine that row-oriented decode has no columnar skip-decode, projection shrinks only Arrow emit — and the tail's Rust-side `nb` decode was never charged at all (every other tail-reading scenario charges it). The trees below apply both fixes: bulk decode uses a flat rate independent of column-projection fraction, and tail acquisition adds a decode charge for 268,435 tail rows at the same [Q-SCAN-ROWS-DERIVED-N1-MMAP] rate (216,000 rows/s ⇒ 1,242.7 ms). Corrected totals: **W1 ~500ms→~1.75s, W2 ~9.73s→~21.3s, W3 ~97.0s→~212.5s (~3.5min)**.

```
W1 — interactive slice (100MB flushed + 128MB tail, 4/20 cols projected)
parse/plan 0.5ms + fan-out 1.5ms
per-node:
   ├─ gen-pin 0.1 ms
   ├─ tail acquisition (corrected):
   │    JVM nb-serialize 128MB/300MB/s[C-JVM-SER,ASSUMED] = 426.7ms + UDS stream 42.7ms[C-UDS,ASSUMED,overlapped]
   │    → transport subtotal ≈ 447 ms
   │    + Rust nb decode of 268,435 tail rows / 216,000 rows/s[Q-SCAN-ROWS-DERIVED-N1-MMAP] = 1,242.7 ms (corrected in verification)
   │    tail_total ≈ 1,689.7 ms
   ├─ bulk scan (corrected, flat rate, no column-fraction discount):
   │    200,000 rows / 216,000 rows/s = 925.9 ms (corrected in verification; was 185.2ms)
   │    io 16.7ms [C-NVME-SEQ] hidden under decode
   ├─ per-node critical path = max(1,689.7, 925.9) = 1,689.7 ms
   ├─ merge: (200,000+268,435) rows × 100ns[ASSUMED] = 46.8 ms
   └─ emit ~2ms [ASSUMED]
per-node total = 0.1+1,689.7+46.8+2 ≈ 1,738.6 ms
+ return 2.5ms
TOTAL ..................................................... ~1.75 s (corrected in verification)
   dominant term: tail decode (now ~97% of per-node cost, wider margin than the uncorrected 89%)
```

```
W2 — table scan (10GB flushed + tail, 8/20 cols)
tail_total (fixed, unchanged): 1,689.7 ms
bulk scan (corrected): 20,000,000 rows / (216,000×4.81[Q-SCAN-LAT-MMAP-N8] rows/s = 1,038,960 rows/s) = 19.25 s (corrected in verification; was 7.70s)
critical path = max(1.69s, 19.25s) = 19.25 s
merge: 20,268,435 rows × 100ns = 2.027 s   (unchanged — row counts didn't move)
emit ~2ms
TOTAL ..................................................... ~21.3 s (corrected in verification)
   dominant term: bulk decode (~90.5% of total)
```

```
W3 — heavy (100GB flushed + tail, 8/20 cols)
tail_total (fixed): 1,689.7 ms
bulk scan (corrected): 200,000,000 rows / 1,038,960 rows/s = 192.5 s (corrected in verification; was 77.0s)
critical path = max(1.69s, 192.5s) = 192.5 s
merge: 200,268,435 rows × 100ns = 20.03 s
emit ~2ms
TOTAL ..................................................... ~212.5 s ≈ 3.5 min (corrected in verification)
   dominant term: bulk decode (~90.6%); merge #2 (~9.4%)
```

**Assumptions:** row width 500 B/row [ASSUMED]; decimal MB/GB throughout [ASSUMED]; C-JVM-SER at 300 MB/s [ASSUMED midpoint — flagged as a 67% divergence from S5's 500 MB/s pick for the same nominal step, §12.6 Correction #8]; C-UDS at 3 GB/s streaming [ASSUMED]; bulk decode-rate proxy = [Q-SCAN-ROWS-DERIVED-N1-MMAP] (no committed nb decode throughput exists); W2/W3 parallel decode applies the measured N=8 *concurrent-scan* multiplier (4.81× [Q-SCAN-LAT-MMAP-N8]) to a *single query's* intra-file parallel decode — flagged by the verifier as an unverified conflation between concurrent-query throughput and single-query core-parallelism, not a validated scaling law (§12.6 reconciled table, last row); merge cost 100 ns/row [ASSUMED, no repo measurement]; emit flat 2ms [ASSUMED]; control-plane constants ~0.5–1.5ms [C-GRPC order, ASSUMED]; tail acquisition and bulk decode modeled as running concurrently (architecture-plausible, not repo-confirmed) so per-node critical path = max, not sum; bulk I/O uses cold [C-NVME-SEQ] at 6 GB/s midpoint.

**Standing costs:** zero write amplification, zero export/cache-rebuild write; the real standing cost is transient — each in-flight query pins the readOrdering barrier and materializes a full extra 128MB nb-serialized copy of the tail in JVM heap for ~447ms of transport (before the now-corrected 1.24s decode charge), i.e. +128MB heap pressure per concurrent query; zero background CPU when idle; no rebuild cost (nothing to rebuild — freshness is definitionally perfect). The pin-duration side-cost is a standing *risk*, not just latency: while pinned the memtable can't safely flush/rotate, and N concurrent queries compete N× for the same barrier, throttling the OLTP write path under sustained OLAP load.

**Dominant-term analysis (corrected):** at W1 the (now much larger) fixed tail cost dominates by an even wider margin than originally claimed — ~97% vs the uncorrected 89% — because the tail's Rust-side decode was previously missing entirely. At W2/W3, corrected bulk decode dominates (~90–91%), with merge a real but distant #2 (~9–19%). The parameter that flips ranking fastest is C-JVM-SER: at 1 GB/s instead of 300 MB/s, tail transport drops toward ~135ms and even W1 would look bulk-decode-dominant; a slower/GC-paused JVM serializer makes the pin-duration risk cost dominate at every scale, not just W1. The single-query-vs-N=8-concurrency conflation used for W2/W3 parallel scaling is an unquantified risk that could move those totals in either direction if the real per-query core-parallelism differs from the measured concurrent-throughput multiplier.

#### S4A — v2 + hot Arrow IPC mmap cache (warm)

**Freshness:** `visible_latency = 0 (already committed to the live TrieMemtable) + tail_acquisition_cost paid by the next query ≈ 592.6 ms`; RPO=0, but every query pays this fixed tax regardless of cache warmth.

```
W1 — interactive slice (100MB flushed, 1 warm gen, 4/20 cols projected; tail 128MB)
parse/plan 0.3ms + fan-out 0.3ms
per-node:
   ├─ gen-pin (1 gen) 0.1ms + zone-map check 0.05ms
   ├─ tail: boundary crossing 0.01ms[C-UDS-analog] + nb decode 128,000 rows/216,000 rows/s[Q-SCAN-ROWS-DERIVED-N1-MMAP] = 592.6ms → 592.61ms
   ├─ bulk mmap scan (warm): 100MB×(4/20)×1.3[envelope,ASSUMED] = 26MB / 8000MB/s[C-PGCACHE] = 3.25ms
   ├─ merge: tail-side 128,000×500ns[ASSUMED] = 64.0ms + bulk-side 100,000×10ns[ASSUMED] = 1.0ms = 65.0ms
   └─ emit 0.3ms
per-node subtotal = 661.3 ms
+ return 0.4ms
TOTAL ..................................................... ~662.3 ms   dominant: tail acquisition (89.6%)
```

```
W2 — table scan (10GB flushed, ~5 gens, 8/20 cols; tail 128MB)
gen-pin(5×0.1)=0.5ms + zone-map(5×0.05)=0.25ms + tail 592.61ms +
bulk mmap: 10,000MB×0.4×1.3=5,200MB / 8000MB/s = 650.0ms + merge: 64.0ms(tail)+10,000,000×10ns=100.0ms(bulk)=164.0ms + emit 0.5ms
per-node subtotal = 1,407.85ms + return 0.4ms
TOTAL ..................................................... ~1.41 s   dominant: bulk scan (46.2%) ≈ tail (42.1%) — the transition point
```

```
W3 — heavy (100GB flushed, ~20 gens, 8/20 cols; tail 128MB)
gen-pin(20×0.1)=2.0ms + zone-map(20×0.05)=1.0ms + tail 592.61ms +
bulk mmap: 100,000MB×0.4×1.3=52,000MB / 8000MB/s = 6,500.0ms + merge: 64.0ms+100,000,000×10ns=1000.0ms=1,064.0ms + emit 1.0ms
per-node subtotal = 8,160.6ms + return 0.4ms
TOTAL ..................................................... ~8.16 s   dominant: bulk scan (79.6%)
```

**Assumptions:** row width ≈1KB/row [ASSUMED]; envelope overhead 30% [ASSUMED]; warm mmap bandwidth 8GB/s midpoint of [C-PGCACHE], no [C-MMAP-FAULT] cost (pages resident); generation counts W1=1/W2=5/W3=20 [ASSUMED]; tail decode uses [Q-SCAN-ROWS-DERIVED-N1-MMAP] on the full fixed 128MB every query (not range-sliced) [ASSUMED]; merge split: tail-side 500ns/row (heavier cross-representation probe) vs bulk-side 10ns/row (vectorizable membership check) [ASSUMED, anchored to C-TRIE-ITER]; emit ≈0.3–1ms [ASSUMED]; control-plane ≈1.0ms total [ASSUMED]; cold/rebuild decode rate 150MB/s [ASSUMED, just above the m1_performance floor]; Arrow IPC write rate ≈4000MB/s [ASSUMED, derated from C-NVME-SEQ].

**Standing costs:** each generation's cache adds ≈1.3× its nb byte size; total footprint vs no-cache baseline ≈2.3× (at W3 scale, ~130GB extra alongside 100GB). Rebuild per generation: `G/150MB/s[decode] + (G×1.3)/4000MB/s[write]` — a representative 5GB generation costs ~34.9s, background but competing with concurrent queries for disk/memory bandwidth. A compaction storm can transiently starve warm-scan bandwidth. Keeping the entire W3 working set warm (~130GB) may exceed node RAM, forcing eviction — a real operational sizing knob. Zone-map sidecar overhead negligible.

**Dominant-term analysis:** at W1, tail acquisition (592.6ms) is ~90% of per-node cost — the columnar cache does almost nothing for interactive queries because it only accelerates the flushed portion (3.25ms). At W2 the terms cross (46% bulk vs 42% tail) — the transition point where scanned volume starts to matter as much as the fixed tail tax. At W3, bulk scan decisively dominates (80%), confirming the cache pays off at scale, not for small scans. Shrinking the tail (more frequent flush) or moving tail decode off the row-oriented `nb` path collapses the ~593ms floor dominating W1/W2; adding scan parallelism shrinks the W3 bulk term and exposes bulk-side merge (13%) as the next bottleneck.

#### S4B — v2 + Parquet+zone-map cache (warm), the v1 pick

**Freshness:** ≈0 ms lag — the tail is scanned live every query; the disposable Parquet cache covers only already-flushed, immutable generations, so no write is ever gated behind a cache rebuild.

> **(corrected in verification)** The original "S4A comparison" lines inside each tree recomputed a synthetic S4A total by holding S4B's own tail/merge numbers constant and swapping in only a bulk-mmap step — this is not S4A's own published total and inverted the headline conclusion (verifier Correction #1, the single most material finding in the review). The corrected comparison below uses **S4A's actual published totals** (662.3ms / 1,408.25ms / 8,161.0ms) in place of the fabricated re-derivation.

```
W1 — interactive slice (100MB flushed touched + 128MB tail, 4/20 cols)
parse/plan 0.5ms + fan-out 0.1ms
per-node:
   ├─ gen-pin 0.1ms
   ├─ tail acquisition (fixed, all W): JNI/IPC transport 128MB/8GB/s[C-PGCACHE mid]=16.0ms + nb parse 64,000 rows/216,000 rows/s[Q-SCAN-ROWS-DERIVED-N1-MMAP]=296.3ms → 312.3ms
   ├─ cache segment fetch (warm, page-cache): 20MB proj / 3×compr[C-PARQ-DEC lit.] = 6.67MB / (4 cores×8GB/s) = 0.21ms
   ├─ columnar decode (CPU-bound, C-PARQ-DEC): 20MB / (4×0.4GB/s/core mid) = 12.5ms (range 5.0–33.3ms @ 0.15–1.0GB/s/core)
   ├─ merge: 114,000 rows / 20M rows/s[ASSUMED] = 5.7ms
   └─ emit 0.3ms
per-node subtotal = 330.8ms (range 324.2–352.5ms) + return 0.3ms
TOTAL ..................................................... ~332 ms (range ~324–353 ms)
   dominant term: tail acquisition, ~94% of total

  ── S4A comparison, corrected (using S4A's own published totals, not a re-derivation) ──
  S4A actual W1 total: 662.3 ms   S4B W1 total: 332 ms
  ratio = 332 / 662.3 ≈ 0.50×  → S4B is ~2× FASTER at W1, not "barely different" (corrected in verification)
```

```
W2 — table scan (10GB flushed/node + tail, 8/20 cols)
tail acquisition (fixed) 312.3ms +
cache fetch: 4GB proj/3×=1.333GB / min(10×8GB/s, 67GB/s ceiling)[C-PGCACHE+DDR5] = 19.9ms +
columnar decode: 4GB / (10×0.4GB/s/core) = 1000ms (range 400–2667ms) +
merge: 5,064,000/20M rows/s = 253.2ms + emit 0.3ms
per-node subtotal = 1,585.3ms (range 986.2–3253.2ms) + return 0.3ms
TOTAL ..................................................... ~1,586 ms (range ~986–3,253 ms)
   dominant term: columnar decode, ~63% of total

  ── S4A comparison, corrected ──
  S4A actual W2 total: 1,408.25 ms   S4B W2 total: 1,586 ms
  ratio = 1,586 / 1,408.25 ≈ 1.13×  → S4B is ~13% slower at W2 (corrected in verification)
```

```
W3 — heavy (100GB flushed/node + tail, 8/20 cols)
tail acquisition (fixed) 312.3ms +
cache fetch: 40GB/3×=13.33GB / 67GB/s[ceiling] = 199.0ms +
columnar decode: 40GB / (10×0.4GB/s/core) = 10,000ms (range 4,000–26,667ms) +
merge: 50,064,000/20M rows/s = 2,503.2ms + emit 0.3ms
per-node subtotal = 13,014.4ms (range 7,015.3–29,682.3ms) + return 0.3ms
TOTAL ..................................................... ~13,015 ms ≈ 13.0 s (range ~7.0–29.7 s)
   dominant term: columnar decode, ~77%; merge #2 at ~19%

  ── S4A comparison, corrected ──
  S4A actual W3 total: 8,161.0 ms   S4B W3 total: 13,015 ms
  ratio = 13,015 / 8,161.0 ≈ 1.60×  → S4B is ~60% slower at W3, not 3.8× (corrected in verification)
```

**Assumptions:** decimal byte units throughout; full-row footprint ≈2KB/row [ASSUMED, converts touched bytes → row counts]; [C-PARQ-DEC] read as decoded/logical GB/s per core, mid=0.4GB/s/core (geometric mean of 0.15–1.0 range); Parquet-vs-nb compression ratio mid=3× [ASSUMED, within stated 2–4× band]; decode parallelism W1=4 cores, W2/W3=10 cores (full row-group fan-out) [ASSUMED]; mmap comparator per-thread mid=8GB/s [C-PGCACHE], capped at a ~64–70GB/s DDR5 ceiling once thread-count×rate exceeds it; tail nb-parse rate reuses [Q-SCAN-ROWS-DERIVED-N1-MMAP] as the best available proxy for parsing already-memory-resident nb bytes; tail JNI/IPC transport modeled memcpy-bound at [C-PGCACHE] (in-process, same-host); merge rate 20M rows/s [ASSUMED, informed by C-TRIE-ITER, picked conservatively low for tombstone-reconcile overhead]; control-plane constants small and fixed [ASSUMED]; Parquet encode rate (rebuild estimate only) ≈0.75× the decode rate mid [ASSUMED, low-confidence, no Table-B source at all].

**Standing costs:** write amplification ≈1.33× (Parquet shadow ≈nb-size/3 per generation, range 1.25–1.5× at 4×/2× compression); memory: zone-map metadata negligible (~78KB/node at W3 scale), but the real win is that S4B's on-disk footprint is 2–4× smaller than S4A's uncompressed mmap cache, so more of the working set fits in a fixed RAM budget; background CPU (rebuild at compaction): at W3 scale, `100GB/(10 cores×0.3GB/s/core)≈33.3s` of CPU-bound work per full-generation compaction, directly competing with foreground decode on the same cores; rebuild-window fallback to raw nb decode (no committed byte-rate; using the >100MB/s target or ≈126,000 rows/s proxy) is slower than either warm path but exists as a fallback — unlike S5.

**Dominant-term analysis (corrected):** at W1 the fixed 128MB tail decode (~312ms) swamps the bulk-decode difference (12.5ms Parquet vs 0.625ms mmap) — but the corrected comparison shows S4B's **total** at W1 is actually ~2× *faster* than S4A's real total (332ms vs 662.3ms), because S4B's assumed tail transport+parse pick (312.3ms) is smaller than S4A's (592.6ms), a difference rooted in unreconciled row-width picks (2KB vs 1KB/row — see §12.6 Correction #6), not a genuine architectural advantage. As scanned bytes grow, the CPU-bound Parquet decode term scales linearly while the tail stays flat, overtaking tail as dominant by W2 and reaching ~77% of total by W3 — narrowing S4B's advantage from a corrected ~2× win at W1 to a corrected ~13% loss at W2 and a corrected ~60% loss at W3 (not the originally claimed 1.04×/2.5×/3.8× progression). The row-width/tail-pick reconciliation gap (Correction #4/#6) means the W1 "S4B wins" result specifically should be treated as an artifact of unreconciled assumptions, not a validated conclusion, until a single tail row-width is pinned across S4A/S4B.

#### S5 — Parquet-attached SSTables (dual-format compaction)

**Freshness:** `T_next_flush − T_write`, avg ≈ `T_flush/2`; no tail path exists — illustrative `T_flush≈60s` [ASSUMED] gives freshness ≈0–60s, avg ~30s, roughly **2–3 orders of magnitude worse** than any tail-merge design. The trees below are the *correctness-patched* variant (tail included) for stop-by-stop comparability against S4B; the scenario *as literally specified* has zero tail cost but returns silently stale results.

```
W1 — interactive slice (100MB flushed touched + 128MB tail, 4/20 cols)
parse/plan 0.5ms + fan-out 0.3ms
per-node:
   ├─ envelope pin 0.15ms [ASSUMED]
   ├─ tail: JVM nb-serialize 128MB/500MB/s[C-JVM-SER]=256.0ms + IPC 128MB/4GB/s[C-UDS]=32.0ms + Rust decode 128MB/1GB/s[~1/8 C-PGCACHE]=128.0ms = 416.0ms
   ├─ bulk scan (Parquet, columnar-pruned): 20MB/(4×0.3GB/s/core[C-PARQ-DEC])=16.7ms + file-open×4gens[ASSUMED]×0.05ms=0.2ms = 16.9ms
   ├─ merge: 760,000 rows × 20ns[ASSUMED]=15.2ms
   └─ emit 0.2ms
per-node total = 448.4ms + return 0.3ms
TOTAL ..................................................... ~449.5 ms (~0.45s)   dominant: tail acquisition (~93%)
```

```
W2 — table scan (10GB flushed/node + tail, 8/20 cols)
tail 416.0ms + bulk: 4,000MB/1200MB/s=3,333.3ms+0.2ms=3,333.5ms + merge: 33,760,000×20ns=675.2ms + emit 0.2ms
per-node total = 4,425.05ms + return 0.3ms
TOTAL ..................................................... ~4,425.85 ms (~4.43s)   dominant: Parquet decode (~75%); merge #2 (~15%)
```

```
W3 — heavy (100GB flushed/node + tail, 8/20 cols)
tail 416.0ms + bulk: 40,000MB/1200MB/s=33,333.3ms+0.2ms=33,333.5ms + merge: 333,760,000×20ns=6,675.2ms + emit 0.2ms
per-node total = 40,425.05ms + return 0.3ms
TOTAL ..................................................... ~40,425.85 ms (~40.4s)   dominant: Parquet decode (~82%); merge #2 (~16.5%)
```

**Assumptions:** row width 300 B/row [ASSUMED]; G_OVERLAP=4 overlapping generations touched per range before compaction consolidates [ASSUMED]; Parquet decode/encode 0.3GB/s/core mid, parallelized across 4 of 10 cores → 1.2GB/s effective [ASSUMED]; column pruning proportional to byte fraction (uniform column width) [ASSUMED]; tail JVM-serialize 500MB/s[C-JVM-SER] + IPC 4GB/s[C-UDS] + Rust in-memory decode 1GB/s[~1/8 C-PGCACHE, prices structural parsing overhead] [ASSUMED throughout — this stop is architecture-fixed, not what differentiates S5]; merge 20ns/row [ASSUMED, explicitly unmeasured anywhere]; file-open cost 0.05ms/file [ASSUMED]; control-plane constants reused nominal.

**Standing costs:** write amplification ≈2× (`BytesWritten_flush = memtable_size×2`, comparable nb/Parquet encoded size [ASSUMED]); compaction write-amp is a strict doubling of whatever the baseline STCS/LCS multiplier is (that baseline itself isn't in either constants table); standing disk footprint ≈2× the nb-only size (upper bound — no measured compression-delta exists to discount it); background CPU: encode over **all 20 columns** (not just the query's projection, since the attachment must serve arbitrary future projections) — 100GB/node at W3 scale costs `100,000MB/1,200MB/s=83.3s` of 4-core CPU per compaction cycle; critically, this rebuild is **mandatory**, not disposable — unlike CQLite's own optional lazy cache with a safe nb-decode fallback, S5's Parquet attachment is the *only* defined read path and blocks compaction-complete with no fallback if it lags; memory: doubled flush-time working set (nb + Parquet buffers concurrently), unquantified [ASSUMED].

**Dominant-term analysis:** at W1 the flat 128MB tail-crossing cost (416ms) dominates (~93%) — identical to every scenario that reads the tail, so S5 gains nothing here vs S4B, it merely also pays it. At W2/W3, Parquet decode dominates (75%→82%) precisely because [C-PARQ-DEC] (0.15–1GB/s/core) is 5–50× slower than the raw NVMe/page-cache bandwidth a pre-materialized Arrow cache (S4A) exploits directly — column pruning's I/O reduction is more than erased by CPU-bound decode at scale. Merge scales linearly with rows touched, a real #2 at W2/W3 (15–16.5%) but not a differentiator since S4B pays a similar order of magnitude. S5 cannot beat S4A/S4B on bulk-scan latency at any of these three scales, and never beats them on freshness or standing write cost.

#### S6 — Continuous columnar replica (TiFlash-style, contrast only)

**Freshness:** mutation-feed hop (node-local IPC, ≈[C-UDS] ~4.5µs) + delta-apply/batch interval (~10ms micro-batch [ASSUMED], no measured value exists) ⇒ **≈10 ms**, dominated by the batching interval, not transport. Requires a CDC-shaped mutation feed **the owner has ruled out** — presented purely as the expensive-end contrast.

```
W1 — interactive slice (100MB flushed + tail, 4/20 cols, per node)
parse/plan 0.5ms + fan-out 0.3ms
per-node: (delta-watermark,stable-gen) pin 0.1ms +
   delta-layer merge (unprunable): 128MB/2GB/s[ASSUMED, cross-checked via C-TRIE-ITER]=62.5ms +
   stable columnar scan (pruned): 100MB×(4/20)=20MB/8GB/s[C-PGCACHE]=2.5ms + emit 0.2ms
per-node subtotal = 65.3ms + return 0.4ms
TOTAL ..................................................... ~66.5 ms   dominant: delta-layer merge — fixed, unprunable floor
```

```
W2 — table scan (10GB flushed + tail, 8/20 cols, per node)
delta merge 62.5ms (unchanged, fixed size) + stable scan: 4GB/6GB/s[C-NVME-SEQ]=667ms + emit 2ms
per-node subtotal = 731.6ms + return 0.4ms
TOTAL ..................................................... ~732.8 ms   dominant: stable-layer scan, now ~10× the delta-merge floor
```

```
W3 — heavy (100GB flushed + tail, 8/20 cols, per node)
delta merge 62.5ms (unchanged) + stable scan: 40GB/6GB/s[C-NVME-SEQ]=6,667ms + emit 5ms
per-node subtotal = 6,734.6ms + return 0.4ms
TOTAL ..................................................... ~6,735.8 ms (~6.74s)   dominant: stable-layer scan, overwhelmingly
```

**Assumptions:** control-plane constants reused (parse/plan 0.5ms, fan-out 0.3ms, return+aggregate 0.4ms) [ASSUMED]; delta buffer pinned at 128MB by analogy to the shared tail parameter, though structurally a separate mutation-feed-fed buffer [ASSUMED]; delta layer is row/wide-block (cheap-append) and NOT column-prunable — full 128MB read+merged every query [ASSUMED, conservative]; delta merge rate 2GB/s, cross-checked via [C-TRIE-ITER]×~200–300B/row discounted from raw [C-PGCACHE] for per-row LWW compare cost [ASSUMED, no measured k-way-merge number exists]; stable-layer scan page-cache-warm [C-PGCACHE] for W1's small working set, NVMe-bound [C-NVME-SEQ] for W2/W3 whose volume plausibly exceeds cache residency [ASSUMED]; emit small fixed constants per workload [ASSUMED].

**Standing costs:** baseline Cassandra write-amp is unchanged, but S6 adds an entirely second write stream: delta append (≈1× mutation payload, durable) plus delta→stable compaction rewrite, with general LSM/columnar-compaction order-of-magnitude assumed at +2–4× rewrite amplification [ASSUMED] ⇒ total added standing write volume ≈3–5× the raw mutation payload, entirely additive to existing row-store cost. A live ~128MB delta buffer is resident *continuously* per hot table/range — standing memory pressure even with zero query traffic, unlike disposable-cache designs. A continuous delta→stable compaction thread runs on the same 10-core node at all times, competing with OLTP write CPU and any concurrent OLAP scan — S6 has **no idle state**, unlike the disposable-cache designs (idle cost=0). Rebuild cost on replica loss/rejoin: ~50s/node at W3 scale (100GB/2GB/s [ASSUMED]), during which that replica must fall back to row-store-only or be excluded from fan-out.

**Dominant-term analysis:** at W1 the fixed 62.5ms delta-merge floor dominates (65.3 of 66.5ms) since the flushed working set is tiny and cache-resident. At W2/W3 the stable-layer sequential scan dominates (667ms, 6.67s) because it scales linearly with flushed bytes×projection ratio while the delta-merge floor stays pinned at 128MB regardless of table size. The flip point is column-projection ratio: narrower projections shrink the stable scan and can re-expose the fixed delta-merge floor even at heavy scale; a slower delta-compaction cadence (larger accumulated delta) raises the floor itself and could make it dominate even at W3.

### 12.4 Cross-scenario comparison

**(a) Summary table** (per-node totals; standing cost condensed to the single most consequential recurring cost per design):

| Scenario | Freshness | W1 total | W2 total | W3 total | Standing cost |
|---|---|---|---|---|---|
| S0 | ~30 min avg / ~60 min worst (cadence-bound) | ~30.7 s | ~47.0 s | ~195.3 s (~3m15s) | Hardlink storage bloat + ~30GB/day extra compaction I/O; separate always-warm Spark cluster; zero incremental mode |
| S1 | Unbounded — flush-rate dependent (~10 min–hours illustrative) | ~1.03 s | ~102.5 s | ~1,025.0 s (~17.1 min) | Zero write-amp; a second always-alive Flight-server process; unbounded staleness risk paid continuously |
| S2 | 43 ms best / 5.04 s worst (dirty-check + cap) | ~1.78 s (cold) | ~80.1 s (cold) | ~791.1 s (~13.19 min, cold) | ~25.6 MB/s/node re-export write-amp under bursty queries; no daemon, idle CPU ≈0 |
| S3 (corrected) | ~0 ms same-replica / ≤tens of ms replica-lag | ~1.75 s | ~21.3 s | ~212.5 s (~3.5 min) | +128 MB JVM heap per concurrent query; readOrdering pin can throttle the OLTP write path under load |
| S4A | ~0 ms same-replica / ≤tens of ms replica-lag (live tail, RPO=0)† | 662.3 ms | 1.41 s | 8.16 s | ~2.3× storage footprint; rebuild CPU competes with concurrent queries; RAM pressure keeping W3 working set warm |
| S4B | ~0 ms same-replica / ≤tens of ms replica-lag (live tail, RPO=0)† | 332 ms | 1.586 s | 13.015 s | ~1.33× storage footprint (smaller than S4A); ~33.3 s/compaction background CPU at W3 scale; has a cold-path fallback |
| S5 | 0–60 s, avg ~30 s (no tail path; 2–3 orders worse than tail designs) | 449.5 ms | 4.43 s | 40.4 s | 2× write-amp, 2× storage floor, mandatory rebuild with **no fallback** — the only design where cache regeneration blocks the write path |
| S6 | ≈10 ms (mutation-feed + micro-batch) — requires ruled-out CDC | 66.5 ms | 732.8 ms | 6.74 s | +3–5× write-amp; 128 MB standing (non-disposable) memory; continuous background CPU, no idle state |

† **Editorial correction (flow-lead consolidation pass):** the consolidator's draft placed S4A/S4B's per-query tail-acquisition tax (≈592.6 ms / ≈312.3 ms) in the freshness column. That tax is **query latency** — already included in the W1–W3 totals — not staleness: S4A/S4B live-stream the tail exactly as S3 does, so their staleness is identical to S3's (~0 same-replica, RPO=0, as each scenario's own "Freshness:" line in §12.3 states). The graph and Pareto reading below are corrected for axis consistency; the uncorrected draft survives in the workflow journal for audit.

**(b) The graph** — freshness (x, log scale) vs. W2 query latency (y, log scale), per-node:

```
   W2 latency (log scale, per-node) →
   100–1000s ┤                                                      ● S1
             │
      10–100s┤  ● S3        ● S2                         ● S0
             │
        1–10s┤  ● S4A,S4B                  ● S5
             │
          <1s┤  ● S6
             └──────┬────────┬────────┬────────┬────────┬─────────────────
                    ms      100ms      1s       10s     100s     1000s+(~17min–2.8hr)
                     freshness (acked-write → OLAP-visible staleness), log scale
```

**Pareto reading (corrected):** **S6 dominates every other scenario** — best latency at ~10 ms freshness — and is excluded a priori by the owner's no-CDC decision; it is a contrast point, not a candidate. Among the architecturally-viable (non-CDC) designs, the corrected axes collapse the frontier to a single cluster: **S4A/S4B sit at the same ~0 ms staleness as S3 while beating its W2 latency by 13–15× (21.3 s → 1.4–1.6 s), so S3-as-an-end-state is Pareto-dominated too.** S0, S1, S2, and S5 remain dominated by S4A/S4B on both axes — their extra staleness tolerance buys no latency advantage on paper. S3 survives not as a destination but as (a) the tail **mechanism inside** S4A/S4B (the live-stream path is identical) and (b) the cold-start / cache-invalidated fallback posture.

**(c) Where the tradeoff actually lives:** with freshness off the table (every live-tail design is ~0 ms stale), the S3 ↔ S4A/S4B choice is **query latency vs standing cost** — a third axis the two-axis chart cannot show: S4A pays ~2.3× storage and rebuild CPU that competes with queries at compaction time; S4B pays ~1.33× storage plus per-query decode CPU; S3 pays nothing when idle. The load-bearing conclusion survives and sharpens: **the disposable per-generation cache, not the tail-crossing mechanism, is where the latency win lives** — and it is bought with standing cost, not with staleness. Past S4A, further staleness tolerance (S5's ~30 s, S1's ~10 min, S0's ~30 min) buys nothing anywhere in the modeled range.

### 12.5 What the numbers decide

**Settled on paper, within this parameter regime:**
- **The live tail's per-query cost matters — decisively, and only at small-scan scale.** Across every design that reads a tail per query (S2, S3-corrected, S4A, S4B, S5), the fixed tail tax is 89–97% of W1 latency and the single reason none of them can hit sub-second interactive latency without shrinking or caching the tail itself. By W2/W3 the same fixed cost is noise (<5%). This settles the shape of the problem: any coordinator-native design's interactive-query story lives or dies on the tail-crossing cost, not the bulk-scan cost.
- **The S4A-vs-S4B gap is real in direction but not trustworthy in magnitude.** Corrected numbers show mmap (S4A) pulling ahead as scale grows (S4B ~13% slower at W2, ~60% slower at W3) — a real, mechanistically-expected direction (CPU-bound Parquet decode vs zero-copy mmap). But the reversed W1 result (S4B ~2× faster) is an artifact of unreconciled tail row-width picks (2KB vs 1KB/row) between the two write-ups, not a genuine architectural finding — it cannot be trusted until row width is pinned to one value.
- **S5's standing cost proves the dual-write design is dominated on every axis it could win on.** It never beats S4A/S4B on latency at any modeled scale, its freshness (0–60s avg) is 2–3 orders of magnitude worse than any tail-merge design, and its 2× write-amp/storage cost is mandatory with no fallback — unlike S4A/S4B's disposable, gracefully-degrading cache. Paying S5's permanent tax buys nothing in this model.

**Cannot be settled without the WS7 bench:**
- The absolute `nb` decode throughput — every no-cache scenario (S1, S2, S3) and every tail-decode step in the cached scenarios (S2, S4A, S4B, S5) rests on a proxy or a stated-but-unmeasured target; there is no committed number anywhere in the repo.
- k-way merge per-row cost — a 25× unreconciled spread across scenarios (10–500 ns/row), explicitly flagged as a total blind spot; the decode-vs-merge dominance crossover point at scale is unknowable without a real bench.
- Parquet decode rate on CQLite's actual schema — [C-PARQ-DEC] is literature-derived (VLDB/FastLanes/BtrBlocks), not a CQLite measurement, and it single-handedly decides the S4A-vs-S4B conclusion at scale.
- Whether single-query parallel decode achieves anything close to the measured N=8 *concurrent-query* throughput multiplier (4.81×) — S3 and S4B both lean on this conflation for their W2/W3 headline numbers, and the verifier flagged it as unvalidated.

**Three most sensitivity-critical constants** (a 5× error would flip a conclusion):
1. **Assumed row-oriented `nb` decode rate** (100–216 MB/s range) — sets the entire per-node latency for every no-cache scenario and every tail-decode step in every cached scenario. A 5× swing moves every W2/W3 total by the same factor and could flip whether S4A/S4B's caching advantage is worth its standing cost at all.
2. **k-way merge per-row cost** (10–500 ns/row, zero measured backing) — a 5× error could make merge, not decode, the dominant term at scale in the cached designs, redirecting where engineering effort belongs (decode vs. merge optimization).
3. **[C-PARQ-DEC]** (0.15–1 GB/s/core, literature not CQLite-measured) — decides the corrected S4A-vs-S4B gap at scale (~1.6× at W3); a 5× higher real rate erases Parquet's CPU tax disadvantage entirely and could reverse the "mmap wins at scale" conclusion outright.

### 12.6 Verifier report

#### Corrections

**1. [MOST MATERIAL — directional inversion] S4B's "vs S4A" comparison uses a fabricated re-derivation of S4A's total, not S4A's own published numbers, inverting the key conclusion.**
S4B's inline "S4A (mmap Arrow) comparison" explicitly holds tail+merge constant at S4B's own values (312.3 ms tail, 2 KB/row) and swaps in only a bulk-mmap-scan step, then reports that synthetic total as "S4A." But S4A's own section computes tail acquisition independently (592.6 ms, 1 KB/row, no separate transport charge) and publishes real totals of **662.3 ms / 1,408.25 ms / 8,161.0 ms** (W1/W2/W3) — not the 319.6 ms / 626.0 ms / 3,413.3 ms S4B's comparison implies.
- Wrong (S4B's stated "OVERALL gap"): W1 ≈1.04x slower, W2 ≈2.5x slower, W3 ≈3.8x slower.
- Right (using S4A's actual published totals, 332/662.3, 1586/1408.25, 13015/8161.0): **W1 ≈0.50x (S4B is ~2x FASTER, not "barely different")**, W2 ≈1.13x slower, W3 ≈1.60x slower.
This flips the paper's own headline claim "tail dominates, format choice barely matters at W1" — under the corrected comparison, format choice is actually the deciding factor at W1 (S4B wins because its assumed tail is smaller/faster), and the W3 gap shrinks from a claimed "confirms the cache's design intent pays off precisely at scale" (3.8x) to a much narrower 1.6x.

**2. S3 (v2 live-stream tail) both mis-scales bulk nb decode by column projection AND omits the Rust-side decode of the tail — headline totals are ~2.2x–3.5x too low.**
- *Column-fraction bug*: S3 computes bulk decode rate as `216,000 rows/s × (20/proj_cols)`, i.e., projecting fewer columns makes row-oriented nb decode *faster*. S2 explicitly states the opposite doctrine for the same row-oriented format: "projection only shrinks the Arrow emit step, not the decode step" — no columnar skip-decode exists in nb. S1 also models decode as flat, unscaled by columns. S3 contradicts both.
- *Missing tail decode*: S3's tail-acquisition tree charges JVM nb-serialize (426.7 ms) + UDS stream (42.7 ms, overlapped) = 447 ms total, then feeds tail rows directly into the k-way merge with **no CQLite-side nb-decode charge at all** — every other scenario that reads a tail (S2, S4A, S4B, S5) charges a separate decode/parse step after transport.
- Wrong → Right (per-node total, applying S1/S2's no-column-discount doctrine to bulk decode, and adding a decode charge for 268,435 tail rows at S3's own cited 216,000 rows/s baseline):
  - W1: **~500 ms → ~1.75 s** (tail becomes ~1.7 s, now dominant by an even wider margin; bulk corrects from 185.2 ms → 925.9 ms)
  - W2: **~9.73 s → ~21.3 s** (bulk corrects from 7.70 s → 19.25 s)
  - W3: **~97.0 s → ~212.5 s (~3.5 min)** (bulk corrects from 77.0 s → 192.5 s)
This doesn't just change magnitude — it removes the illusion that S3's tail-tax is a one-time 447 ms curiosity; corrected, it's ~1.7 s and the true floor under every workload.

**3. S1's "dominant-term analysis" 10x-decode-improvement claim is arithmetically wrong.**
S1 claims: "even a 10× decode-rate improvement (100→1000 MB/s) would make merge (25 ms) the new floor... put the query solidly in interactive territory (~30 ms vs ~1s today)."
- Wrong: implies decode(1000 MB/s of 100 MB) ≤ merge (25 ms).
- Right: decode = 100 MB / 1000 MB/s = **100 ms**, still ~4x merge's 25 ms — decode remains dominant (~79% of total), and total would be **~127 ms**, not "~30 ms." Reaching a ~30 ms total requires roughly a **40–50x** decode improvement (≥4,000–20,000 MB/s), not 10x.

**4. Cross-scenario "128 MB tail acquisition" produces five different numbers for the same nominal operation.**
S2: 744.2 ms (decode only, mmap-MB/s basis) · S3: 447 ms (no Rust decode charged — see #2) · S4A: 592.6 ms (1 KB/row) · S4B: 312.3 ms (2 KB/row) · S5: 416 ms (three-stage JVM/IPC/Rust breakdown). The spread (312–744 ms, 2.4x) is driven almost entirely by inconsistent row-width picks applied to the *same* cited [Q-SCAN-ROWS-DERIVED-N1-MMAP] rate (216,000 rows/s): at 200 B/row that implies ~43 MB/s; at 2 KB/row it implies ~432 MB/s — a 10x spread in the *implied* MB/s throughput of one constant. See reconciled table.

**5. S1 vs S2 backend-rate inconsistency for identical "no-cache, per-query nb decode" architecture.**
S1 models bulk decode at a flat 100 MB/s ("D_decode," unspecified backend). S2 — same no-cache full-decode architecture — computes both a buffered (100 MB/s) and mmap (172 MB/s) figure and uses **172 MB/s throughout**. Applying S2's own mmap rate to S1: W1 decode = 100 MB/172 MB/s = **581.4 ms, not 1000 ms** (42% lower); total ≈607 ms, not ~1027 ms. This is >20% and shifts S1's numbers materially without any stated justification for why S1 uses the slower, unqualified figure while S2 (same operation) prefers mmap.

**6. Row-width assumption drift, 10x spread, for the identical nominal "20-column mixed-type row."**
S1/S2: 200 B/row · S5: 300 B/row · S3: 500 B/row · S4A: 1,000 B/row · S4B: 2,000 B/row. This single unreconciled input multiplies straight through every merge-cost and (in S3/S4A/S4B) tail-decode-time calculation across the document — it is the root cause of Correction #4 and inflates/deflates merge costs by up to 10x scenario-to-scenario for "the same" 100 MB/10 GB/100 GB workload points.

**7. k-way merge per-row cost: 25x unreconciled spread, all [ASSUMED] with zero repo backing.**
S5: 20 ns/row · S4A(bulk-side): 10 ns/row · S1: 50 ns/row · S4B: 50 ns/row · S3: 100 ns/row · S2: 300 ns/row · S4A(tail-side): 500 ns/row. Every one of these is explicitly flagged as unmeasured (the "Missing Measurements" list calls out merge cost as a total blind spot), yet the ratio of high to low pick (500 ns vs 10 ns = 50x) swings the merge term's share of total latency by an order of magnitude scenario-to-scenario for supposedly the same operation.

**8. C-JVM-SER point-pick divergence for the same tail-serialize step.**
S3 picks 300 MB/s; S5 picks 500 MB/s — a 67% difference for the same cited range ("100s of MB/s," no exact figure) applied to the same nominal 128 MB tail-serialize operation.

**9. Cold/buffered nb-decode-rate divergence for the same stated repo target.**
S1 and S2's buffered path both use 100 MB/s (literal reading of the `>100MB/s` m1_performance target). S4A's cold-cache fallback instead uses 150 MB/s ("just above the stated design floor"), a 50% higher pick for what should be the identical unmeasured quantity.

#### Reconciled cross-scenario table

| step | reconciled value | scenarios affected |
|---|---|---|
| 128 MB tail acquisition (transport + decode) | No single number survives reconciliation — root cause is row-width drift (200 B–2 KB/row) applied to the same [Q-SCAN-ROWS-DERIVED-N1-MMAP] rate; adopting one row-width (e.g. 500 B/row, S3's pick) and charging both transport AND decode consistently gives ≈**1.2–1.3 s**, not the 312–744 ms range currently shown | S2, S3, S4A, S4B, S5 |
| Rust-side nb decode of tail bytes | Must be charged separately from any JVM-side serialize/transport step — S3 currently charges 0 ms for this | S3 (missing), vs. S2/S4A/S4B/S5 (present) |
| Row width for 20-col mixed schema | Pick one value (recommend 500 B/row, mid of the 200 B–2 KB range in use) | S1/S2 (200B), S3 (500B), S4A (1KB), S4B (2KB), S5 (300B) |
| Row-oriented (nb) bulk decode rate, no cache | Should NOT scale with column-projection fraction (S2's own stated doctrine); reconciled bulk decode = flat rate × bytes, independent of columns | S1 (100 MB/s, correct treatment) vs. S2 (100/172 MB/s, correct treatment) vs. S3 (incorrectly speeds up 2.5–5x with fewer columns) |
| Bulk decode backend choice (buffered vs mmap) for "no-cache, per-query decode" architecture | Pick mmap (172 MB/s) consistently if that's the intended backend — currently S1 alone omits it | S1 (100 MB/s) vs. S2 (172 MB/s) |
| k-way merge per-row cost | No measured value exists anywhere in-repo; recommend converging on ~50–100 ns/row (mid of the 10–500 ns spread in use) pending a real merge bench | S1(50), S2(300), S3(100), S4A(10/500 split), S4B(50), S5(20) |
| C-JVM-SER pick for tail serialize | Pick one point in the "100s of MB/s" range consistently (currently 300 vs 500 MB/s) | S3, S5 |
| Cold/buffered nb decode rate (design-floor target) | Both are literal readings of the same `>100MB/s` target; reconcile to 100 MB/s unless a documented reason exists for S4A's 150 MB/s uplift | S1, S2 (100 MB/s) vs. S4A cold-fallback (150 MB/s) |
| S4A "real" total vs. S4B's synthetic comparison baseline | S4A actual totals: W1 662.3 ms, W2 1,408.25 ms, W3 8,161.0 ms — use these, not S4B's internally re-derived 319.6/626.0/3,413.3 ms, for any "S4A vs S4B" gap claim | S4A, S4B |
| MB/GB unit convention (decimal vs binary) | Standardize on decimal (10⁶/10⁹) throughout; currently mixed even within single scenarios | S0 (binary), S3 (binary tail / decimal bulk — internally mixed), S6 (binary delta-merge calc / decimal stable-scan calc — internally mixed); S1/S2/S4A/S4B/S5 already decimal |
| N=8 concurrent-scan speedup (4.81x) applied to *single-query* intra-file parallel decode | This constant measures 8 independent concurrent queries, not one query parallelized across cores/files — using it for single-query speedup is an unverified conflation; flag as a documented risk, not a validated scaling law | S3 only |

#### Verdict

- **S0**: Internally arithmetic-consistent (all subtotals and dominant-term percentages check out against its own formulas). Its 10GB/100GB figures use *binary* MB conversion (10,240/102,400 MB) while nearly every other scenario uses decimal — immaterial to S0 alone (<3%) but breaks apples-to-apples comparison with S1–S6. **Reliable as-is for internal use; flag unit convention if compared cross-scenario.**
- **S1**: Arithmetic is internally correct throughout the latency trees. The "Dominant-term analysis" paragraph (Correction #3) contains a real logic/arithmetic error and should be restated. Its bulk-decode rate is also inconsistent with S2's mmap choice for the identical architecture (Correction #5). **Needs Correction #3 applied verbatim; totals themselves (1027 ms/102.5 s/17.1 min) stand unless reconciled to S2's mmap backend, in which case they drop ~40%.**
- **S2**: Fully arithmetic-consistent — every subtotal, dominant-term percentage, and total checks out exactly against its stated formulas. Assumption picks (300 ns/row merge, 200B/row) are its own defensible choices but diverge from other scenarios (see reconciled table). **Reliable as-is.**
- **S3**: Contains the most material errors in the set — a genuine column-fraction-scaling bug plus a fully missing decode-cost line for the tail (Correction #2). **Do not use published totals (500 ms/9.73 s/97.0 s) without applying the correction; corrected totals are ~1.75 s/~21.3 s/~212.5 s.**
- **S4A**: Internally arithmetic-consistent (every subtotal and percentage verified exactly). Its own published totals (662.3 ms/1.41 s/8.16 s) are correct and should be the reference point for any S4A-vs-S4B comparison. **Reliable as-is.**
- **S4B**: Its own latency trees are internally arithmetic-consistent (verified exactly, aside from sub-1%-rounding noise). However its comparative framing against S4A (Correction #1) is broken and produces a misleading, in one case direction-inverting, conclusion. **Trees reliable as standalone numbers; discard/redo every "S4A comparison" line and the "OVERALL gap" figures.**
- **S5**: Fully arithmetic-consistent internally (every subtotal verified). Row-width (300B) and C-JVM-SER pick (500MB/s) diverge from siblings (see table) but don't invalidate S5's own totals. **Reliable as-is.**
- **S6**: Arithmetic-consistent (every subtotal/total verified), aside from a small internal binary/decimal unit slip in the delta-merge calc (64 vs 62.5 ms, ~2.4%, immaterial). **Reliable as-is.**

**Overall**: Six of eight scenario write-ups are internally arithmetic-sound; the two structural failures are S3 (self-contained, ~2.2–3.5x understated) and the S4B-vs-S4A cross-comparison (uses mismatched baselines, direction-inverting at W1). The deeper, document-wide problem is not arithmetic but **unreconciled shared constants** — row width, tail-decode rate, merge-cost rate, and MB/GB convention all drift 2x–10x across scenarios that are explicitly meant to be compared apples-to-apples on identical workload points (100 MB/10 GB/100 GB + 128 MB tail, 6-node fan-out). Any comparative conclusion drawn across scenarios (S1 vs S3 vs S4A vs S4B vs S5) should be treated as provisional until these constants are pinned to single values.

### 12.7 Addendum: S4A′ — range-sliced tail acquisition (flow-lead refinement, 2026-07-05)

Prompted by the owner's question of whether S6's dominance justifies reconsidering the no-CDC decision. Every S4x tree above charges each query for acquiring the **full 128 MB tail** — an assumption the simulators flagged themselves (`tail decode uses ... the full fixed 128MB every query (not range-sliced) [ASSUMED]`). But the tail need not be acquired whole: Cassandra's standard Memtable read API (`makePartitionIterator(ColumnFilter, DataRange)`) is exactly how Cassandra's own range reads consult the memtable, and the trie iterates in token order — so a token-range-scoped OLAP scan can open a **range-bounded tail iterator** whose cost scales with the query's range fraction *f*, not the memtable size. (WS2 must verify the plugin can use this under the same `readOrdering` pin as `getFlushSet` — flagged, not assumed settled.)

**S4A′ arithmetic** (same constants and assumptions as S4A; only the tail term changes to `128 MB × f`):

W1 (touches 100 MB of a 10 GB node → f ≈ 1%; tail slice ≈ 1.28 MB ≈ 1,280 rows @1KB [ASSUMED, same as S4A]):

```
per-node:
   ├─ gen-pin + zone-map ................................. 0.15 ms
   ├─ tail (range-sliced):
   │    trie range-iterate 1,280 rows @10M rows/s [C-TRIE-ITER] ≈ 0.13 ms
   │    JVM serialize 1.28 MB @300 MB/s [C-JVM-SER] .......... ≈ 4.3 ms
   │    UDS transfer [C-UDS] ................................. ≈ 0.01 ms
   │    engine nb decode 1,280 rows @216k rows/s [Q-SCAN-ROWS-DERIVED-N1-MMAP] ≈ 5.9 ms
   │    tail subtotal ..................................... ≈ 10.4 ms   (was 592.6 ms)
   ├─ bulk mmap scan (warm) ............................... 3.25 ms
   ├─ merge: tail 1,280×500ns + bulk 100,000×10ns ......... 1.64 ms
   └─ emit ................................................ 0.3 ms
per-node subtotal ≈ 15.7 ms;  cluster TOTAL ≈ ~17 ms   dominant: tail JVM-serialize + decode (65%)
```

W2/W3 are full scans (f = 100%): unchanged from S4A (1.41 s / 8.16 s) — range-slicing buys nothing when the query touches everything.

**Corrected comparison vs S6:**

| | W1 interactive | W2 table scan | W3 heavy | Freshness | Standing cost |
|---|---|---|---|---|---|
| S4A′ (no CDC) | **~17 ms** | 1.41 s | 8.16 s | ~0 ms + replica lag | 2.3× storage, rebuild at compaction, idle-cheap |
| S6 (needs mutation feed) | 66.5 ms | 0.73 s | 6.74 s | ~10 ms (apply lag) | 3–5× write-amp [ASSUMED], continuous CPU, non-disposable state, no idle state |

**What this decides about the CDC question:** S6's headline win was W1 — and S4A′ takes W1 outright *on paper* once the tail is range-sliced, at zero standing write-path cost. S6's remaining edge is ~1.2–1.9× at full-scan scale (0.73 s vs 1.41 s at W2), where absolute latencies are seconds and the buyer pays 3–5× write amplification, a continuously-running apply path on the OLTP node, and a mutation feed: either commitlog CDC (with its known operational hazard — a lagging consumer fills `cdc_raw` and Cassandra **rejects writes**) or an in-process write-tee in the plugin (no CDC pipeline, but per-write overhead on the OLTP hot path — a direct G3 risk, and feasibility of observing puts in the TrieMemtable subclass is unverified). Paper verdict: **the no-CDC decision survives these numbers** — but the W2-scale gap is real, so the honest posture is to re-pose the question after WS7 benches the three flip-risk constants (§12.5), since a 5× error in merge or decode rates could widen S6's edge. Caveats: S4A′ inherits every [ASSUMED] tag of S4A, plus the range-bounded-iterator-under-pin verification now added to WS2.

