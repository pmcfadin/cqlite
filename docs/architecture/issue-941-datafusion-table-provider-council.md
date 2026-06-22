# Issue 941 DataFusion Table Provider Council

**Date:** 2026-06-22
**Issue:** [#941](https://github.com/pmcfadin/cqlite/issues/941) - Add data fusion table provider
**Status:** Architecture analysis and design alternatives
**Audience:** CQLite maintainers, Trino connector maintainers, Cassandra operators, DataFusion integrators

## Issue statement

Issue #941 proposes:

> Once we can bulk read correctly and efficiently let's add a table provider that uses the sidecar to pull the ring info, take a snapshot, and do the read, with the predicate push down goodness.

The key design question is not whether DataFusion can expose a table. It can. The hard question is where the provider belongs in a massive Cassandra cluster when Trino is already doing MPP scheduling, and how to preserve Cassandra storage semantics while making the scan fast enough to justify bypassing CQL.

## Council

Four specialist reviews informed this document:

| Role | Main position |
|---|---|
| DataFusion provider specialist | A `TableProvider` should be a leaf scan provider over snapshot manifests. Provider-over-Flight is the lowest-risk first step because it reuses the existing node-local `cqlite-flight` data plane. |
| Cassandra/SSTable/Sidecar specialist | The v1 contract must be a best-effort analytics snapshot from one selected healthy replica per token interval. Do not claim Cassandra `QUORUM` or silently switch replicas mid-query. |
| Trino/MPP specialist | Do not insert DataFusion as a second MPP engine inside Trino. Trino must own split planning and page production through its connector SPI. |
| Performance specialist | Co-located local snapshot reads are the best live-pull design. Materialized epochs are better for repeated analytics when freshness can lag. One split per vnode is pathological until token seeks exist. |

## External references

The designs are grounded in current primary sources:

- DataFusion `TableProvider` requires `schema`, `table_type`, and `scan`, and its scan receives projection, filters, and limit while returning an `ExecutionPlan`: [docs.rs DataFusion TableProvider](https://docs.rs/datafusion/latest/datafusion/catalog/trait.TableProvider.html).
- DataFusion `ExecutionPlan::execute(partition, TaskContext)` returns a streaming `RecordBatch` stream and its docs call out cancellation/resource cleanup requirements: [docs.rs DataFusion ExecutionPlan](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html).
- Arrow Flight's `GetFlightInfo` returns endpoints/tickets and clients consume endpoint streams with `DoGet`: [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html).
- Trino connectors push filters by returning a new table handle plus residual filters, build distributed splits in `ConnectorSplitManager`, and feed requested columns through page sources: [Trino connector development docs](https://trino.io/docs/current/develop/connectors.html).
- Apache Cassandra Sidecar exposes ring, schema, snapshots, and token-range replicas. Endpoint constants are in [ApiEndpointsV1.java](https://github.com/apache/cassandra-sidecar/blob/trunk/client-common/src/main/java/org/apache/cassandra/sidecar/common/ApiEndpointsV1.java), and `TokenRangeReplicasResponse` distinguishes read replicas, write replicas, `(start, end]` token bounds, and replica state/status metadata in [TokenRangeReplicasResponse.java](https://github.com/apache/cassandra-sidecar/blob/trunk/client-common/src/main/java/org/apache/cassandra/sidecar/common/response/TokenRangeReplicasResponse.java).

Local context used:

- [CQLite -> Arrow Flight -> Trino plan](../flight-trino/PLAN.md)
- [Flight/Trino journal](../flight-trino/JOURNAL.md)
- [Fast Cassandra analytics over Arrow Flight design](../plans/2026-06-17-cassandra-fast-analytics-arrow-flight-design.md)
- [Aggregation pushdown benefit evaluation](../plans/2026-06-20-issue-841-aggregation-pushdown-benefit-eval.md)
- [Sidecar Parquet projection position](cassandra-sidecar-parquet-projections.md)
- `cqlite-flight/src/ticket.rs`, `filter.rs`, `producer.rs`, `service.rs`
- `trino-connector/src/main/java/com/rustyrazorblade/cqlite/flight/*`

## Current implementation baseline

The repository already contains more of the live analytics stack than the older Flight/Trino plan implies:

- `cqlite-flight` serves `GetFlightInfo`, `GetSchema`, and `DoGet`.
- `FlightTicket` carries keyspace, table, DDL, optional snapshot name, `(start, end]` token bounds, projection columns, a recursive predicate tree, and aggregation specs.
- `MergeProducer` uses `KWayMerger`, reconstructs full `QueryRow` values for predicate evaluation, applies projection only at Arrow conversion, prunes SSTables using Summary token spans where possible, and keeps a row-level token filter as a correctness backstop.
- `cqlite-core::export::arrow_convert` exposes reusable CQL-to-Arrow schema and `RecordBatch` conversion.
- The Trino connector calls Sidecar for schema, ring, and token-range replicas, creates one split per token range pinned to one replica, converts Arrow vectors into Trino pages, and has filter and aggregation pushdown hooks.

Important gaps remain:

- `cqlite-flight::service::do_get` still materializes `Vec<RecordBatch>` before encoding the Flight stream. That is not acceptable for massive scans.
- Snapshot directory resolution exists in `cqlite-flight`, but the Trino page source still builds tickets with no snapshot name. Production must create a Sidecar snapshot epoch, include it in tickets, and clean it up.
- The connector's split model is one range per Sidecar token range. Correct, but inefficient at vnode scale if each split scans the same local SSTables again.
- The current Trino README status lags source in some pushdown areas; treat source code and tests as authoritative.
- CQLite's logical reconciliation has known hard cases that must remain explicit in the correctness contract: equal timestamp rules, complex collection path deletes, counters, wide partitions, and TTL reference time.

## Non-negotiable invariants

These apply to all three ideas.

1. **Exactly-once token coverage:** every Sidecar read replica range `(start, end]` must appear exactly once in the query plan. No gaps. No overlaps. No RF duplicate reads.
2. **One selected replica per interval in v1:** pick one healthy `UP/NORMAL` read replica per canonical interval. If it fails mid-query, fail the query and release the snapshot lease. Do not silently switch replicas unless the consistency contract is deliberately weakened.
3. **Snapshot, not live files:** production scans must read immutable Sidecar snapshot component sets. Live directories are a dev/test fallback only.
4. **Snapshot epoch manifest:** a query-visible epoch must bind keyspace, table, table UUID/incarnation, schema digest, partitioner, token coverage, selected replicas, snapshot name, component list, component sizes/checksums where available, and TTL reference time.
5. **Predicate exactness:** push predicates only when CQLite fully enforces the same semantics. Return residuals to DataFusion or Trino for every unsupported expression.
6. **Reconciliation order:** token/key pruning can happen before row assembly. Non-key predicates run after per-partition reconciliation, tombstones, TTL, and delete shadowing.
7. **Internal projection is wider than output projection:** always read keys, predicate-only columns, and reconciliation metadata even if the output projection excludes them.
8. **Bounded streaming:** row-count batching alone is insufficient. Batches need row and byte caps, stream cancellation, backpressure, and admission control.
9. **No DataFusion inside Trino as an MPP scheduler:** Trino's connector SPI already owns split scheduling, residual filtering, aggregation pushdown decisions, and page production.

## Three design ideas

| Idea | Summary | Best use | Main risk | Detailed doc |
|---|---|---|---|---|
| A. Co-located snapshot scan provider | DataFusion provider plans Sidecar snapshots and token splits, then each partition pulls Arrow batches from node-local `cqlite-flight`. Trino keeps its existing MPP connector path but shares the same scan contract. | Fresh-ish interactive analytics with local Cassandra volume access. | Current Flight path must become truly streaming and snapshot lifecycle must be implemented. | [Design A](issue-941-design-a-colocated-flight-provider.md) |
| B. Remote Sidecar range provider | DataFusion workers use Sidecar snapshot component APIs and HTTP range reads, with CQLite reading through a bounded remote file abstraction. No node-local Flight service required. | Environments where analytics workers cannot mount Cassandra data volumes. | Sidecar and network become the data plane; small range reads can destroy performance. | [Design B](issue-941-design-b-remote-sidecar-range-provider.md) |
| C. Materialized snapshot epoch provider | Sidecar snapshots feed a CQLite materialization job that writes current-row Arrow/Parquet/Iceberg fragments by epoch; DataFusion or Trino reads the materialized table. | Massive repeated analytics, dashboards, and workloads where freshness can lag. | More storage and delayed freshness; must atomically publish epochs and not mix partial outputs. | [Design C](issue-941-design-c-materialized-epoch-provider.md) |

## Recommendation

Build **Design A** first, but define it as a shared scan contract, not only a DataFusion feature. The immediate issue #941 deliverable should be a `DataFusion TableProvider` that:

- uses Sidecar for schema, topology, token-range read replicas, and snapshot epoch creation;
- creates DataFusion execution partitions from grouped token ranges pinned to selected replicas;
- uses existing `cqlite-flight` tickets for node-local `DoGet` scans;
- reports exact/unsupported filter pushdown conservatively;
- returns residual filters to DataFusion;
- enforces cancellation and snapshot lease cleanup.

For Trino, do not route queries through DataFusion. Keep the Java connector as the MPP integration because Trino requires its own split and `Page` model. Instead, share the scan manifest, snapshot lease logic, predicate capability rules, ticket format, and metrics between DataFusion and Trino.

Promote **Design B** only when local volume access is not viable. It is operationally attractive but performance-fragile.

Use **Design C** for very large or repeated analytics where protecting Cassandra foreground disks matters more than query freshness. It is the only design that can make dashboard workloads cheap at scale without repeatedly scanning operational SSTables.

## Massive cluster behavior

The naive plan "one vnode token range equals one query split" is correct but not scalable. In a 500-node Cassandra cluster with 16 vnodes and RF=3, the Sidecar read replica map may expose thousands of intervals. If each interval becomes an independent local SSTable merge, the same table's SSTables may be opened and walked many times per node.

Until CQLite has efficient token seeks, the planner should group adjacent selected intervals per replica so each node/table snapshot is scanned as close to once per query as possible. The node-local scanner still filters emitted partitions by interval membership to preserve exactly-once output. After token seeks mature, split by estimated bytes and wall-clock time rather than vnode count.

Recommended live-scan guardrails:

- default 1-2 active analytics scans per Cassandra node;
- default analytics disk budget around 50-100 MiB/s per node, or no more than 10% of device bandwidth;
- default analytics CPU budget around 10-15% per node;
- refuse or degrade when SSTable fan-in exceeds 64-128 for a table/node unless an operator override is set;
- refuse new epochs when snapshot pinned bytes exceed 5-10% of data directory capacity;
- stop or throttle when Cassandra p99 read/write latency regresses by 10-15% during analytics load.

These numbers are starting defaults, not universal truths. They must become configurable admission-control policy backed by metrics.

## Trino MPP interaction

Trino should see this as a normal connector scan:

```text
Trino coordinator
  -> ConnectorMetadata.applyFilter/applyProjection/applyAggregation
  -> ConnectorSplitManager: token range groups pinned to replicas
  -> workers run ConnectorPageSource instances
  -> each PageSource pulls Arrow Flight streams or materialized table fragments
  -> Arrow vectors become Trino Page/Block values
```

Do not place a DataFusion query plan between Trino and Cassandra. That would add a second scheduler, split model, memory model, and residual-filter contract while still requiring conversion into Trino pages.

Aggregation pushdown should remain gated by measured reduction. Existing local evaluation for issue #841 shows massive wins for global and low-cardinality aggregates, but a loss when group count approaches row count. The planner should push `count`, `sum`, `min`, `max`, and `avg` only when estimated `rows_after_filter / groups` is at least roughly 4-10.

## Metrics required before production claims

Planning:

- token coverage percent, gaps, overlaps;
- selected replica by token interval and datacenter;
- selected replica state/status;
- snapshot create time and cleanup time;
- schema digest and partitioner;
- epoch age and TTL reference time.

Scan:

- SSTables listed, opened, pruned, and skipped;
- compressed and uncompressed bytes read;
- disk read MB/s and queue time;
- chunks decompressed;
- Summary/Index/Statistics cache hit rate;
- SSTable fan-in p50/p95/max.

Merge and correctness:

- partitions, rows, cells, tombstones, TTL expirations, range tombstones;
- merge heap operations;
- peak active partition bytes;
- equal timestamp conflicts;
- unsupported type/predicate counters.

Pushdown and output:

- exact, inexact, residual, unsupported predicate counts;
- token prune ratio;
- predicate selectivity;
- projection column ratio;
- aggregation input rows, groups, and reduction ratio;
- Arrow rows and bytes per batch;
- time to first batch;
- Flight bytes or materialized bytes;
- cancellation latency and backpressure time.

Contention:

- Cassandra p50/p95/p99 read/write latency;
- pending compactions;
- flush latency;
- disk utilization and await;
- analytics process CPU/RSS;
- admission-control rejections by reason.

## Open decisions

- Whether issue #941 should land as a new crate, for example `cqlite-datafusion`, or inside `cqlite-flight` behind a `datafusion` feature.
- Whether to align the workspace from Arrow 53 to the Arrow version required by the chosen DataFusion release, or isolate DataFusion from CQLite internals via Arrow IPC/Flight.
- Whether the first provider supports only exact filters, or also marks some filters as inexact with residual evaluation.
- Whether snapshot epoch ownership lives in the provider, a small shared Rust crate, or a Sidecar-facing service used by both DataFusion and Trino.
- Whether Trino and DataFusion share a language-neutral JSON scan manifest that extends the existing `FlightTicket`, or each connector has a separate internal model.

---

## Claude Council Review (2026-06-22)

A second council — five Claude specialist reviewers plus a code-grounding pass against the actual repo — reviewed this packet independently. This section records their findings and rebuttal so the original authors can re-engage. **Overall position: the direction is endorsed (build A first, keep Trino as the MPP owner, DataFusion as a leaf scan surface). The notes are about hardening, not redirection.**

### Code-grounding (verified against current source)

Every factual claim in "Current implementation baseline" is accurate:

- `FlightTicket` carries all claimed fields — `cqlite-flight/src/ticket.rs:219-261`.
- `producer.produce()` returns `Vec<RecordBatch>` (`producer.rs:312`) and `do_get` collects it before encoding (`service.rs:186-198`); an in-source comment already calls streaming a "later optimization."
- The Trino page source builds tickets with `Optional.empty()` snapshot / live-dir reads — `CqliteFlightPageSourceProvider.java:69`.
- Workspace is uniformly Arrow 53; no `datafusion` dependency exists yet.
- **One nuance the doc omits:** `FlightTicket` *already* has a `wraparound: bool` field and a `token_in_half_open_range` helper, so min-token wrap handling is partially present in code. The design text should make the membership predicate and full-ring coverage test explicit and confirm whether Sidecar emits the wrap as one interval or two.

### DataFusion/Arrow — two contract gaps to promote to invariants

1. **Statistics are absent.** `TableProvider::scan` returns an `ExecutionPlan` whose `statistics()`/`partition_statistics()` feed the cost-based optimizer (join order, TopK vs sort, repartition). Add an invariant that the plan reports at least `Precision::Inexact` row/byte estimates from snapshot/`Statistics.db` sizes.
2. **"Avoid Inexact in v1" is backwards.** `Inexact` is the *safe* pushdown — DataFusion keeps a `FilterExec` above the scan and re-applies the predicate, so it can never corrupt results. Only a wrong `Exact` is a correctness bug. Default uncertain-but-translatable predicates to `Inexact`; reserve `Unsupported` for what cannot be translated at all.

### Cassandra/Sidecar — three items must move from prose into the non-negotiable invariants

1. **A Cassandra snapshot is not a consistent cut** — it is per-node hardlinks at the instant the endpoint runs; each replica's snapshot is a different instant over different unrepaired data. State plainly: per-range point reads at independent capture instants; no cross-range/cross-replica consistency, atomicity, or monotonicity.
2. **One-replica reads may be missing acknowledged writes and may contain rows other replicas already deleted** (no read-repair, no digest reconciliation, unreplayed hints).
3. **Counters are wrong by construction** — a single replica holds only its counter shard. Mark unsupported in v1.

The reconciliation hard-cases list is also incomplete: add `gc_grace`/purgeable-tombstone resurrection, range-tombstone shadowing across SSTables, static rows, partition-level deletes, secondary indexes (read base), MVs, and LWT/Paxos (in-flight state is invisible to an SSTable scan).

### Trino/MPP — thesis endorsed, two gaps

The "no DataFusion-as-second-scheduler inside Trino" thesis is correct. Gaps: (1) **split-to-host affinity is never addressed** — co-location requires `ConnectorSplit.getAddresses()` returning the co-located worker plus a stated `NodeSelectionStrategy` (likely `HARD_AFFINITY`); (2) **dynamic filtering is unmentioned** — the highest-leverage Trino feature for star-joins; fold dynamic filters into ticket predicates / token pruning. Also name explicitly that "DataFusion behind Trino's Arrow Flight connector as a per-endpoint page source" is acceptable and distinct from the rejected "DataFusion as planner inside Trino."

### Performance — the `max()` model under-weights two dominant terms

**Decompression throughput** (split out per-codec; for Zstd it routinely beats disk) and the **CPU of reconstructing full `QueryRow` values before projection** (confirmed `producer.rs:470-472` — projection pushdown saves Arrow-convert + ship cost but not decode CPU, the usually-dominant term; push the projected+predicate-only column set into row assembly). The **p99-feedback throttle is an unspecified closed control loop** — a naive proportional throttle on noisy p99 oscillates. Specify an AIMD governor with a dead-band, ≥30-60 s minimum dwell, and a hard floor of zero scans; treat it as a safety cutout, not a fine-grained rate controller.

### Open decisions worth resolving early

- The Arrow-53-vs-DataFusion version gap: the Flight IPC boundary is a real mitigation, but pin conservative IPC writer options on the server and add a cross-version Flight round-trip test — don't assume the boundary is free.
- Manifest ownership/versioning: version it with reject-on-unknown-major, generate Java+Rust types from one source of truth, and **sign the manifest itself** (not just per-node tickets) — a tampered manifest reassigns token coverage = silent data loss/duplication.
