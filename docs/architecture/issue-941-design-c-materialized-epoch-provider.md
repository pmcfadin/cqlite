# Issue 941 Design C: Materialized Snapshot Epoch Provider

**Status:** Strategic design for repeated analytics and operational isolation
**Related overview:** [Issue 941 council analysis](issue-941-datafusion-table-provider-council.md)
**Core idea:** Use Sidecar snapshots and CQLite to publish immutable current-row analytics epochs, then expose those epochs to DataFusion and Trino as Arrow/Parquet/Iceberg tables.

## Executive summary

This design does not make every Trino query scan Cassandra SSTables. Instead, an epoch builder periodically or explicitly:

1. uses Sidecar to create snapshots;
2. selects one healthy replica per token interval;
3. reconciles SSTables with CQLite;
4. writes materialized current-row fragments by token range;
5. atomically publishes an epoch manifest;
6. lets DataFusion or Trino query the published epoch.

```text
Sidecar snapshots
  -> CQLite epoch builder
  -> reconciled current-row Arrow/Parquet/Iceberg fragments
  -> atomic epoch manifest
  -> DataFusion TableProvider / Trino Iceberg or Parquet connector
```

This trades freshness for lower operational risk and better repeated-query economics. It is the best design for dashboards and large repeated scans because Cassandra foreground disks are not touched for every query.

## When to use it

Use this design when:

- the workload repeats queries over the same large tables;
- dashboards tolerate bounded staleness;
- protecting Cassandra p99 latency is more important than freshest possible data;
- object storage or a shared filesystem is available;
- the organization already uses Iceberg/Delta/Hive table formats;
- query engines include Trino, Spark, DataFusion, and Python.

Do not use this as the only answer to issue #941 if users require live-ish ad hoc reads over fresh snapshots. It is a complementary production path.

## Architecture

### Epoch builder

The epoch builder is a separate service or job:

- plans token coverage from Sidecar `readReplicas`;
- creates snapshots on selected replicas;
- validates snapshot manifests;
- scans each selected node's local snapshots or Sidecar component ranges;
- reconciles rows with CQLite;
- writes output fragments;
- publishes a manifest atomically;
- cleans up Sidecar snapshots after publish or failure.

The builder can run on Cassandra nodes, in a Kubernetes job with read-only mounts, or in a remote environment using Design B's Sidecar range reader.

### Output layout

For Parquet or Arrow IPC fragments:

```text
s3://bucket/cqlite-epochs/
  keyspace=<ks>/
    table=<table>/
      epoch=<epoch-id>/
        manifest.json
        data/
          token_bucket=00000/part-00000.parquet
          token_bucket=00001/part-00000.parquet
          ...
```

For Iceberg:

```text
warehouse/
  <catalog>/<namespace>/<table>/
    metadata/
    data/epoch=<epoch-id>/token_bucket=<bucket>/...
```

Token bucket partitioning should be stable and independent of Cassandra vnode boundaries. A practical first layout is 256-4096 token buckets, sized by output bytes rather than vnode count.

### Epoch manifest

`manifest.json` must include:

- epoch id;
- created-at and published-at times;
- source keyspace/table/table UUID;
- schema digest and CQL DDL;
- partitioner;
- TTL reference time;
- selected token intervals and replicas;
- Sidecar snapshot names;
- input component manifests;
- output fragment paths;
- row counts and byte counts;
- min/max token per fragment;
- checksums or object-store ETags;
- status: building, published, failed, cleanup-complete.

Consumers must only read epochs with `status = published`.

### DataFusion provider

The DataFusion provider becomes simpler than live SSTable scans:

- list published epochs;
- choose latest epoch unless caller pins one;
- expose Arrow schema from the manifest;
- delegate scan to Parquet/DataFusion listing execution;
- use DataFusion's native projection, predicate, limit, and aggregate capabilities over columnar files.

This provider does not need to understand Sidecar during query execution if epoch discovery is in the manifest registry.

### Trino path

The preferred Trino path is not the CQLite Flight connector. It is a table-format connector:

- Iceberg connector if epochs publish Iceberg tables;
- Hive/Parquet connector if epochs publish partitioned Parquet with metastore registration;
- a small CQLite epoch connector only if no catalog is available.

For large repeated analytics, using Trino's mature table-format connectors is usually better than building another live connector path.

## Freshness and consistency

V1 contract:

```text
Queries read one atomically published analytics epoch. The epoch contains
current-row materialization from exactly one selected healthy read replica per
canonical token interval, using one TTL reference time and one schema digest.
```

This is still not Cassandra `QUORUM`, but it is stronger for analytics than live per-query snapshot windows because every query reads a stable, named epoch.

Freshness options:

- scheduled epochs, for example every 5 minutes;
- explicit on-demand epoch builds;
- hybrid: scheduled base epochs plus on-demand urgent rebuilds;
- future incremental epochs using delta-scan or commitlog CDC.

Do not expose a partially built epoch. Atomic publish is mandatory.

## Reconciliation choices

There are two coherent output semantics.

### Current-row materialization

The builder reconciles all selected SSTables and writes one live row per logical row.

Pros:

- easiest for Trino and DataFusion users;
- deletes, TTL, and last-write-wins are resolved before query;
- normal Parquet/Iceberg predicate pushdown works.

Cons:

- full rebuilds can be expensive;
- incremental maintenance requires a real merge/update protocol.

This is the recommended v1 semantic.

### Delta materialization

The builder writes change-like records from new SSTables and expects downstream merge logic.

Pros:

- cheaper incremental ingestion;
- aligns with append-only lake patterns.

Cons:

- tombstones, TTL, writetime, collection replacement, and equal-timestamp ties become downstream responsibilities;
- naive Trino queries can resurrect deleted data;
- every consumer must understand CQLite delta semantics.

Do not use delta semantics as the default table provider unless the table name and docs make it explicit.

## Massive cluster behavior

This design scales differently from live pulls:

- build jobs can be rate-limited independently of query concurrency;
- repeated queries reuse the same materialized bytes;
- compaction/reconciliation cost is paid once per epoch;
- Trino scans object storage or a lakehouse table instead of Cassandra disks;
- failures affect epoch freshness, not active Cassandra query p99.

Planning still uses the same Cassandra invariants:

- one selected healthy read replica per token interval;
- complete coverage;
- no overlaps;
- schema digest stability;
- snapshot cleanup.

Output partitioning should target analytics engines, not Cassandra topology:

- bucket by token range for build parallelism and pruning;
- keep Parquet files in the 128-512 MiB range after compression;
- avoid millions of small files;
- store min/max token and key statistics per fragment;
- compact small output files outside Cassandra.

## Pushdown behavior

Live CQLite pushdown is replaced by columnar/table-format pushdown:

- projection pushdown reads only needed Parquet columns;
- predicate pushdown uses Parquet row-group stats and page indexes where available;
- partition pruning uses token bucket, epoch, and optional user-defined partitions;
- aggregation pushdown depends on Trino/DataFusion/table-format support, not CQLite Flight.

This is usually better for repeated analytics because data is already columnar. It is worse for the freshest possible read because a new epoch must be built first.

## Performance model

Build cost:

```text
epoch_build_time ~= max(
  snapshot_create_time,
  source_scan_bytes / source_scan_budget,
  reconciliation_events * log2(sstable_fan_in),
  output_bytes / object_store_write_budget,
  table_commit_time
)
```

Query cost:

```text
query_time ~= max(
  selected_parquet_bytes / object_store_read_budget,
  decoded_column_values * decode_cost,
  engine_shuffle_bytes / query_network_budget
)
```

Materialization wins when:

```text
queries_per_epoch * live_scan_cost > epoch_build_cost + queries_per_epoch * materialized_query_cost
```

It loses when queries are rare and freshness is mandatory.

## Operational guardrails

Builder:

- build at most one epoch per table at a time;
- cap per-node source scan bandwidth;
- cap snapshot pinned bytes;
- pause when Cassandra p99 latency or pending compactions exceed thresholds;
- publish only after output checksums and row counts are recorded;
- cleanup Sidecar snapshots even if object-store commit fails.

Storage:

- lifecycle old epochs by retention policy;
- keep enough epochs for rollback/debugging;
- compact small files;
- track storage cost per table.

Consumers:

- default to latest published epoch;
- allow pinning an epoch for reproducibility;
- expose epoch id in query metadata;
- never mix fragments from multiple epochs in one table scan unless explicitly implementing time travel.

## Failure handling

| Failure | Behavior |
|---|---|
| selected replica unavailable before build | pick another healthy replica and record it before publish |
| selected replica fails during build | fail epoch, cleanup snapshots, leave previous epoch active |
| partial output write | mark epoch failed; delete or quarantine fragments |
| schema changes mid-build | fail epoch and retry with new schema |
| publish commit fails | do not expose epoch; cleanup source snapshots |
| query while build running | read previous published epoch |
| object-store read error | engine query fails or retries per table-format semantics |

## Security

- Epoch builder needs Sidecar credentials for snapshots and component reads.
- Query engines need object-store/catalog credentials, not Cassandra filesystem access.
- Published manifests should not include sensitive local filesystem paths beyond component identities needed for audit.
- Access control can be applied at table/catalog level using existing Trino/Iceberg policies.

## Required CQLite work

1. A streaming current-row reconciliation API with a stable scan manifest.
2. Exact Cassandra semantics for tombstones, TTL reference time, collection element paths, and equal timestamp ties before claiming full correctness.
3. Row and byte-capped Arrow/Parquet writers.
4. Token bucket output writer.
5. Epoch manifest writer and validator.
6. Optional Iceberg writer or integration with an existing Iceberg writer.
7. Differential validation against Cassandra reads and `sstabledump` fixtures.

## Validation

Correctness:

- full epoch row counts match Cassandra reference after flush/quiescence;
- deletes and TTLs do not resurrect;
- static rows and range tombstones match fixtures;
- complex collections match expected current state;
- schema changes fail cleanly;
- epoch manifest coverage has no token gaps/overlaps.

Performance:

- one build amortizes across repeated Trino queries;
- output file sizes stay in target range;
- object-store scan is faster than repeated live SSTable scan for dashboard workloads;
- builder throttling protects Cassandra latency.

Operational:

- failed epoch leaves previous published epoch readable;
- cleanup removes Sidecar snapshots;
- retention removes old epochs;
- pinned epoch queries are reproducible.

## Pros and cons

Pros:

- cheapest repeated-query path;
- protects Cassandra foreground disks from every dashboard refresh;
- uses mature Trino/DataFusion columnar scan paths;
- works across many query engines;
- easier query-time predicate/projection pushdown.

Cons:

- data is stale by epoch cadence;
- requires extra storage and an epoch builder;
- current-row materialization can be expensive;
- incremental correctness is hard;
- less aligned with issue #941's immediate "take a snapshot and read" live-provider phrasing.

## Recommendation

Treat this as the production-scale companion to Design A. Build Design A for live interactive reads and use Design C for large repeated analytics. If real user workloads are dashboard-heavy, Design C should become the default production recommendation even if Design A lands first.

---

## Claude Council Review (2026-06-22)

**Verdict: the strongest long-term design for the most likely workload (dashboards / repeated scans), and the only one that escapes per-query decompression + reconciliation cost.** Make it the production default for repeated analytics, with Design A for fresh ad-hoc reads. Ship current-row only; keep delta experimental. The council was notably more unanimous in its praise here than for A or B.

### Trino/MPP — "use Iceberg, not a custom connector" is the single best call in the packet

Routing query-time access through Trino's mature Iceberg/Hive connectors inherits, for free, everything a bespoke live connector must reimplement and keep correct across Trino releases: data-size-based split planning, manifest/partition pruning, min/max + page-index pushdown, dynamic filtering, `applyAggregation`, snapshot isolation / time-travel, and node-agnostic scheduling. Current-row materialization resolves tombstones/TTL/LWW before query, so naive Trino SQL cannot resurrect deleted data — the correctness burden collapses to one builder instead of every query. **But prefer a real Iceberg catalog (HMS/Glue/REST) over "partitioned Parquet + metastore"** for true atomic publish + snapshot isolation. State the routing rule explicitly: dashboards/repeated → C; ad-hoc fresh → A.

### Cassandra/Sidecar — current-row is the right v1; the delta path is worse than the doc admits

- **Current-row materialization as v1 is correct.** But the delta hazard is sharper than stated: once a delta epoch is published and a source tombstone is later purged past `gc_grace_seconds` (or not carried forward), a downstream merge that never saw that tombstone resurrects the row **permanently**. If delta is ever shipped it must carry tombstones/range-tombstones/partition-deletes as first-class records *and* use `gc_grace`-aware scheduling. Mark it **experimental**, not just "explicit."
- **Counters are more dangerous here than in a live read** — materialized output looks authoritative but a single replica's counter shard is not the cluster value. **Exclude counter tables from materialization**, or label them per-replica-shard.
- **Current-row still inherits single-replica incompleteness** — can miss acked writes on a non-selected replica, can include rows another replica already deleted. The manifest must record "single-replica-per-range, no read-repair."
- Confirm partition keys never split across token buckets, or LWW skew appears at bucket boundaries.

### Performance — the build-vs-freshness constraint is under-modeled

- This is the only design that pays decompression + full-row reconciliation **once per epoch** instead of per query — it directly fixes the `producer.rs` full-row-decode penalty (`producer.rs:470-472`) that A and B incur on every query. That is the real headline.
- **A full current-row rebuild re-reconciles the entire table every epoch.** At 5-minute cadence on a large table, build time can exceed the interval and epochs fall behind. State the rebuild-time-vs-cadence constraint; require longer cadence for large tables or incremental epochs (delta-scan) before claiming "5-minute freshness at scale."
- **Frame the decision as load shape, not just per-query cost** — a build job is rate-limitable and bounded; N ad-hoc live queries are not.
- Bucket by output bytes (the doc says this) but allow **adaptive bucket split/merge on rebuild** rather than a fixed 256-4096 count, or the 128-512 MiB file target and the fixed bucket count conflict under wide-partition skew.

### DataFusion/Arrow — best design for the optimizer, and the custom code nearly disappears

- Materializing to Parquet/Iceberg hands DataFusion a columnar table where projection, row-group/page-index pushdown, and `Statistics` all work for free via `ListingTable`/`ParquetExec` — the exact surfaces A and B are missing.
- The provider section (lines 106-117) is the thinnest and slightly oversells the work: name `ListingTable` + partition columns for `token_bucket`/`epoch`; the provider's real job is epoch **discovery** + handing files to the built-in Parquet scan.
- **`iceberg-rust` writer support is young** — make Parquet + Hive-style partitioning the v1 output, Iceberg explicitly phase 2.
- **Schema evolution across epochs is unaddressed** — DataFusion rejects scans mixing fragments with differing Arrow schemas. Specify consumer-side handling when the schema digest changes between epochs.

**Bottom line:** the production-scale companion to A and probably the eventual default for real workloads. Ship current-row only, on a real Iceberg catalog (Parquet-first if Iceberg-Rust isn't ready), exclude counters, mark delta experimental with mandatory tombstone-carry + `gc_grace`-aware scheduling, and pin down the rebuild-time-vs-freshness-cadence story before claiming freshness SLAs at scale.
