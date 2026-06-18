# Research and Design: Fast Cassandra Analytics over Arrow Flight

**Status:** Research proposal — pending maintainer review

**Date:** 2026-06-17

**Audience:** CQLite, Apache Cassandra Sidecar, and query-engine integrators

**Project boundary:** a new sibling project; CQLite and Sidecar remain independent dependencies

## Executive decision

Build a separate, co-located analytics service that turns immutable Cassandra
snapshots into Arrow Flight streams. Use Apache Cassandra Sidecar as the control
plane for schema, topology, replica ownership, health, and snapshot lifecycle.
After the CQLite work identified below, use it as the data plane for SSTable
decoding, read-time reconciliation, filtering, projection, and Arrow
`RecordBatch` construction.

The proposed fast path is:

```text
Sidecar snapshot + token map
        -> token-range pruning
        -> read-only multi-SSTable reconciliation
        -> predicate + projection
        -> Arrow RecordBatch stream
        -> Arrow Flight
        -> PrestoDB or Trino
```

Here, **compact** means a read-only logical merge. The service must not trigger
physical Cassandra compaction for each query. Physical compaction would compete
with Cassandra for disk and CPU, add unpredictable latency, and mutate the
operational system.

This direction is feasible, but CQLite does not yet implement the critical
middle of the path. It has mature Arrow type conversion and a bounded result
channel, but some legacy compressed scans still buffer the complete
decompressed data section. The default read path does not reconcile versions
across SSTable generations. It also lacks token-range scans and a public Arrow
batch stream. Those are correctness requirements, not optional optimizations.

For the shortest query-engine proof of concept, target **PrestoDB first**.
PrestoDB 0.292+ includes a base Arrow Flight connector that already turns
Flight endpoints into distributed splits. Trino does not provide an equivalent
generic Arrow Flight connector; it needs a custom connector that converts Arrow
vectors into Trino `Page`/`Block` objects. Keep the Flight contract
engine-neutral so the Trino connector can follow without changing the service.

## What the original proposal gets right

The statement “compact -> filter -> Arrow Flight” identifies the correct major
stages. It also correctly treats topology as a separate concern from SSTable
decoding. Cassandra already distributes data by token range, so a query engine
can parallelize a table scan without routing every row through CQL coordinators.
Arrow Flight can then preserve batches across the network instead of converting
them to row-oriented JSON, CQL protocol frames, or JDBC values.

Two qualifications matter:

1. Trino does not generically “speak Arrow” at its connector boundary. Its SPI
   asks a connector for Trino pages. A Trino connector may consume Arrow, but it
   still owns the conversion. [Trino's connector documentation](https://trino.io/docs/current/develop/connectors.html)
   describes `ConnectorSplitManager`, `ConnectorPageSource`, `SourcePage`,
   predicate pushdown, and projection pushdown in those terms.
2. Arrow reduces row-wise serialization, materialization, and copying after
   rows have been decoded. Arrow IPC encoding, gRPC framing, and network
   transfer remain. It does not make Cassandra SSTables columnar. A narrow
   projection can avoid allocating and converting unneeded cells, but the
   scanner still walks row-oriented SSTable bytes. The likely speedup comes
   from local sequential reads, distributed token scans, early row
   elimination, bounded batching, and avoiding Cassandra's normal request
   path.

## Existing precedent: Apache Cassandra Analytics

This architecture does not need to invent Sidecar-driven analytics from
scratch. [Apache Cassandra Analytics](https://github.com/apache/cassandra-analytics)
already uses Sidecar to run Spark bulk reads. Its documented flow supplies
Sidecar contact points, keyspace, table, datacenter, and snapshot options, then
reads Cassandra into Spark without using normal CQL row reads. The
[`CassandraDataLayer`](https://github.com/apache/cassandra-analytics/blob/trunk/cassandra-analytics-core/src/main/java/org/apache/cassandra/spark/data/CassandraDataLayer.java)
and
[`SidecarProvisionedSSTable`](https://github.com/apache/cassandra-analytics/blob/trunk/cassandra-analytics-core/src/main/java/org/apache/cassandra/spark/data/SidecarProvisionedSSTable.java)
show the reusable pattern: obtain schema and ring data, create snapshots, split
the token ring, choose replicas, list SSTable components, and read ranges.

The new project should reuse that control flow and, where practical, its
Sidecar client behavior. Its distinct contribution is a Rust/CQLite scan engine
that emits Arrow batches over Flight and an adapter for interactive distributed
SQL engines.

## Project boundaries

### CQLite

CQLite should own storage semantics and a network-independent analytics scan
API:

- Open an explicit, immutable SSTable manifest.
- Restrict scans to one or more Murmur3 token intervals.
- Reconcile all versions of a partition using Cassandra's per-cell rules.
- Apply partition, clustering, and scalar predicates without dropping residuals.
- Decode only columns needed by output, predicates, keys, and reconciliation.
- Produce a bounded stream of Arrow `RecordBatch` values.
- Report scan counters and deterministic errors.

CQLite should not own Sidecar clients, Flight authentication, snapshot leases,
query-engine plugins, or cluster scheduling.

### Apache Cassandra Sidecar

Sidecar should remain the Cassandra control plane and safe file-access plane:

- Schema and Cassandra settings.
- Ring topology and node health.
- Read replicas for each `(start, end]` token range.
- Snapshot creation, listing, component streaming, and cleanup.
- Authentication, authorization, and transfer throttling for its APIs.

Sidecar is not currently an Arrow server, query worker, or general process
dispatcher. Its lifecycle and operational-job APIs run defined Cassandra
operations; they do not provide arbitrary command execution. Adding a CQLite
route directly to Sidecar would require a custom build or upstream module, not
installing a runtime plugin. The proof of concept should therefore run under a
normal process supervisor, a companion container in the Cassandra pod, or a
systemd unit. A generic Kubernetes DaemonSet often cannot mount the RWO volume
used by a Cassandra pod; it works only with an explicit host-path or compatible
storage design.

### New sibling project

A working name is `cassandra-arrow-analytics`. It should own:

- A small Flight control endpoint that turns a scan request into distributed
  `FlightEndpoint` values.
- A per-node Flight `DoGet` service backed by CQLite.
- Sidecar topology, schema, snapshot, and health clients.
- Snapshot lease and cleanup logic.
- Replica selection and token-range grouping.
- PrestoDB plugin code, followed by a Trino connector if required.
- TLS, authentication propagation, signed tickets, limits, cancellation,
  metrics, deployment manifests, and end-to-end tests.

A possible repository layout is:

```text
cassandra-arrow-analytics/
  flight-service/       # Rust gateway + per-node DoGet service
  presto-plugin/        # Java extension of presto-base-arrow-flight
  trino-connector/      # Java connector, added after the PoC
  deploy/               # Docker Compose, Helm, systemd examples
  integration-tests/    # Cassandra + Sidecar + engine test cluster
  benchmarks/
```

### V1 non-goals

- No physical Cassandra compaction, repair, mutation, or arbitrary process
  execution.
- No cluster-wide transactional snapshot or emulated CQL consistency level.
- No general SQL parser in the Flight service and no Flight SQL surface.
- No aggregate, join, Top-N, or dynamic-filter pushdown.
- No Sidecar fork or runtime plugin requirement.
- No counters, Cassandra 5 vectors, or unvalidated schema evolution.
- No CDC/change-stream semantics; the existing delta-scan work remains a
  separate pipeline.

## Sidecar capabilities to use

The current Sidecar route definitions are authoritative in
[`ApiEndpointsV1`](https://github.com/apache/cassandra-sidecar/blob/trunk/client-common/src/main/java/org/apache/cassandra/sidecar/common/ApiEndpointsV1.java).

| Need | Sidecar endpoint |
|---|---|
| Cluster ring | `GET /api/v1/cassandra/ring` |
| Keyspace-aware ring | `GET /api/v1/cassandra/ring/keyspaces/:keyspace` |
| Read/write replica map | `GET /api/v1/keyspaces/:keyspace/token-range-replicas` |
| CQL schema | `GET /api/v1/keyspaces/:keyspace/schema` |
| Version and partitioner | `GET /api/v1/cassandra/settings` |
| Create snapshot | `PUT /api/v1/keyspaces/:ks/tables/:table/snapshots/:name` |
| List snapshot components | `GET` on the snapshot route |
| Stream a component | `GET .../snapshots/:name/components/:component` |
| Clear snapshot | `DELETE` on the snapshot route |
| API discovery | `/spec/openapi.json`, `/spec/openapi.yaml`, `/openapi/` |

The
[`TokenRangeReplicasResponse`](https://github.com/apache/cassandra-sidecar/blob/trunk/client-common/src/main/java/org/apache/cassandra/sidecar/common/response/TokenRangeReplicasResponse.java)
contains separate read and write replicas, `(start, end]` token boundaries,
datacenter grouping, and node state/status. That is enough to assign every ring
interval to one healthy replica and avoid reading replication-factor copies.

The
[`SSTablesAccessModule`](https://github.com/apache/cassandra-sidecar/blob/trunk/server/src/main/java/org/apache/cassandra/sidecar/modules/SSTablesAccessModule.java)
supports snapshot creation, listing, deletion, and component streaming.
Component responses support HTTP ranges through Sidecar's
[`FileStreamHandler`](https://github.com/apache/cassandra-sidecar/blob/trunk/server/src/main/java/org/apache/cassandra/sidecar/handlers/FileStreamHandler.java).
Component requests must carry the manifest's `dataDirectoryIndex`, and the
table path must preserve the table name including its UUID. Snapshot deletion
ignores the table parameter and clears the named snapshot across the keyspace,
so snapshot names must be globally unique within a keyspace.

Sidecar caches snapshot listings for two hours by default, and deletion does
not invalidate that cache. Never reuse a snapshot name. A unique epoch name
prevents a recreated snapshot from receiving a stale manifest.

Sidecar snapshot APIs are node-local. The new coordinator must fan requests out,
verify full ring coverage, and clean up partial work after failures. Snapshots
across nodes are staggered rather than transactional, so the service must state
its consistency contract precisely.

Replica mapping depends on healthy gossip. Treat `UP/NORMAL` as a planning hint,
exclude joining, leaving, moving, and replacing nodes, and confirm that each
selected node still exposes the required snapshot before issuing splits.

## Recommended architecture

```text
                         metadata / split planning
  PrestoDB coordinator  ----------------------------+
  or Trino coordinator                               |
           |                                         v
           | connector request               Flight gateway
           |                                  |  - schema
           |                                  |  - topology
           |                                  |  - snapshot lease
           |                                  |  - replica selection
           |                                  +----------+
           |                                             |
           | FlightInfo: one endpoint per node-grouped    | Sidecar REST
           | token-range set                              |
           v                                             v
  Query-engine workers                         Cassandra Sidecars
       |          |                                   |
       | DoGet   | DoGet                              | CQL/JMX/local files
       v          v                                   v
  node A Flight  node B Flight  ...              Cassandra nodes
       |              |
       | CQLite       | CQLite
       v              v
  local snapshot  local snapshot
       \______________  ______________/
                      \/
             Arrow RecordBatch streams
```

The gateway can be the same Rust binary as the node service, exposed through a
stable service address. `GetFlightInfo` performs planning and returns endpoints
whose locations point directly at the chosen node services. Query workers then
use `DoGet` against those nodes, so result bytes do not pass through the
gateway.

The ticket should be opaque and signed. It should identify the snapshot lease,
table and schema digest, assigned token intervals, projected columns, supported
predicates, reference time, batch limits, and deadline. It should never expose
an arbitrary filesystem path.

## End-to-end query flow

1. The query engine parses SQL and identifies the table, required columns,
   `TupleDomain`/predicate constraints, and limit hints.
2. The connector sends a typed scan descriptor to the Flight gateway. The
   service should not accept arbitrary SQL as its core contract.
3. The gateway fetches the keyspace schema, Cassandra settings, and token-range
   replica map from Sidecar. During rolling upgrades, it checks settings on
   every selected instance and validates every listed SSTable descriptor and
   version; node settings alone do not identify every on-disk format.
4. The snapshot lease manager selects a pre-created, complete epoch from the
   sibling project's durable registry and captures one `read_time_micros` for
   deterministic TTL handling. It must not create cluster snapshots inside
   `GetFlightInfo`: Presto's base client makes a blocking `getInfo()` call and
   does not use `PollFlightInfo`. Snapshot creation belongs in a background
   cadence or a separate asynchronous action.
5. The planner chooses exactly one healthy, local-datacenter read replica for
   each canonical token interval. It groups many intervals assigned to the same
   node into one or a small number of splits; one full scan per vnode would be
   pathological while CQLite lacks token seeks.
6. `GetFlightInfo` returns one Flight endpoint per split. Each ticket is
   self-contained or backed by durable query state. V1 retries remain pinned to
   the original replica and snapshot; switching to another eventually
   consistent replica can change the rows and is not a deterministic retry.
7. A query worker calls `DoGet` on the selected node service.
8. The node service resolves the snapshot manifest, opens the local SSTables,
   and executes a CQLite `SnapshotScanPlan`.
9. CQLite performs token pruning first. For each partition, it merges versions
   across SSTables, applies partition/row/range/cell tombstones and TTL at the
   captured reference time, then evaluates non-key predicates against the live
   row.
10. CQLite builds projected Arrow batches and streams them with backpressure.
11. PrestoDB's Arrow connector or a Trino connector converts vectors to the
    engine's native pages. The query engine performs residual filters, joins,
    aggregates, sorts, and final limits.
12. Cancellation must propagate from the engine through Flight to the active
    CQLite cursor. Closing a client stream alone does not guarantee server-side
    termination. The coordinator then releases its registry lease and schedules
    cleanup.

The order in step 9 is required. Filtering an old cell before reconciliation
can change the live result. Only token restrictions and safe key/index pruning
may run before the logical merge.

## Snapshot and consistency contract

Never scan Cassandra's live SSTable set without a lease. Cassandra compaction
can replace and unlink files while an external process reads them. Sidecar
snapshots create stable hard links and provide a manifest that remains valid for
the query.

Creating a flushing snapshot for every interactive SQL query would add latency
and operational load. Sidecar does not provide a snapshot epoch registry,
reader reference counts, or a “latest complete” lookup. The sibling project
must provide a durable registry containing unique snapshot names, target nodes,
completion state, schema digest, creation/reference times, and active readers.
The production shape should use that registry as a snapshot lease manager:

- Create snapshots on a configurable cadence or for an explicit benchmark/job.
- Publish an epoch only after every required node reports success.
- Reuse the newest complete epoch while it is inside the freshness objective.
- Give each epoch a Sidecar TTL longer than the maximum query duration where
  supported, always perform explicit cleanup, and never let TTL expiry race an
  active `DoGet`.
- Reject new epochs when disk-pressure thresholds are crossed.

The v1 consistency promise should be:

> Results are a best-effort, read-only snapshot assembled from one selected
> replica per token range over a bounded snapshot capture window.

This is not a cluster-wide transactional snapshot and does not provide a CQL
consistency level such as `QUORUM`. A selected replica can be stale because of
repair state, hinted handoff, or ordinary eventual consistency. Snapshotting all
eligible replicas permits failover, but does not make the snapshots atomic.

Schema also needs a lease. The Sidecar schema endpoint reflects driver metadata
and does not prove agreement by itself. Check per-node schema versions before
publishing an epoch, then capture the CQL schema and digest. A digest of the
current schema still does not prove that older SSTables are compatible; v1
should reject schema-evolved snapshots unless their serialization headers and
dropped-column history have been validated.

## Data-access modes

### 1. Local snapshot volumes — recommended first CQLite PoC

Run the Flight service on every Cassandra node with read-only access to the
snapshot directories. Sidecar creates and lists snapshots; the local service
maps Sidecar's data-directory index and component names onto configured,
read-only mounts.

This matches CQLite's current local-file API and keeps SSTable bytes off the
network. It also provides the clearest test of whether CQLite can saturate local
storage and Arrow Flight can keep up.

The trade-off is filesystem coupling. Sidecar does not expose a supported
absolute-local-path API, so deployment configuration must keep Sidecar and the
Flight service's data-directory mappings consistent. Preserve the manifest's
table UUID and data-directory index to avoid reading the wrong table incarnation
after drop/recreate. This mode also bypasses Sidecar's stream authorization and
bandwidth throttles; OS permissions, cgroups, and the Flight service's own
admission controls must secure and throttle direct reads.

### 2. Sidecar HTTP range reads — compatibility mode

Run central or query-engine-local workers and read snapshot components through
Sidecar HTTP ranges. This follows Apache Cassandra Analytics, avoids filesystem
permissions, and works when the analytics service cannot share Cassandra
volumes.

CQLite would need a positioned asynchronous file abstraction backed by HTTP
range requests. Downloading entire components to temporary files is acceptable
for a transport spike, but it is not the intended fast path.

### 3. Materialized Parquet/Iceberg copy — fallback architecture

Use the existing CQLite Parquet work to publish an external analytical copy and
query it through the engine's Iceberg or Hive connector. This has the strongest
query-engine integration and the weakest freshness. It also moves merge and
delete correctness into a separate table-format pipeline. Iceberg/Delta
provides transactional file metadata, not Cassandra LWW or tombstone
semantics; an explicit downstream reconciliation job is still required. The
existing
[`cassandra-sidecar-parquet-projections.md`](../architecture/cassandra-sidecar-parquet-projections.md)
and [delta-scan epic #696](https://github.com/pmcfadin/cqlite/issues/696)
cover that separate CDC-oriented path.

## Query-engine choice

### PrestoDB: shortest proof of transport

[PrestoDB's base Arrow Flight connector](https://prestodb.io/docs/current/connector/base-arrow-flight.html),
added in
[release 0.292](https://github.com/prestodb/presto/blob/master/presto-docs/src/main/sphinx/release/release-0.292.rst),
already supplies a `ConnectorPageSource`, Arrow-vector-to-Presto-block
conversion, and split creation from `FlightInfo.endpoints`. Its
[`ArrowSplitManager`](https://github.com/prestodb/presto/blob/master/presto-base-arrow-flight/src/main/java/com/facebook/plugin/arrow/ArrowSplitManager.java)
maps each Flight endpoint to a Presto split. Its layout handle carries desired
columns and a `TupleDomain`, so a small Cassandra-specific plugin can encode
projection and safe predicate constraints into the Flight descriptor.

The base module is not a zero-code generic catalog. The new project must still
implement schema/table discovery, authentication call options, descriptor
encoding, and any custom type handling. Java PrestoDB workers can use the
connector without Presto C++. Native Presto/Prestissimo is an optional second
benchmark path and must be compiled with
`-DPRESTO_ENABLE_ARROW_FLIGHT_CONNECTOR=ON`. Its Velox connector can import
Arrow arrays with ownership transfer, so it is likely to show a higher
transport ceiling than the Java path, which converts vectors into Presto
blocks.

The base connector deliberately returns its complete `TupleDomain` as
unenforced, so Presto evaluates it again even when the descriptor asks the
Flight service to filter. That is the correct safe default for a PoC. The base
module has no limit, Top-N, aggregate, or join pushdown; no split locality
preference; weak multi-location failover; and incomplete Java page-source
metrics. The Java base has no dynamic-filter path, and the native connector
explicitly rejects dynamic filters. These are follow-up work, not reasons to
bypass its useful split and page-source scaffold.

The current generic `ArrowBlockBuilder` supports integers, floats, booleans,
dates, timestamps, decimals, strings, binary, lists, maps, and structs. CQLite's
`Time64(Nanosecond)` and UUID `FixedSizeBinary(16)` need explicit compatibility
work. Fixed-size binary requires custom conversion or an alternate wire
encoding. Cassandra nanosecond time cannot map losslessly to PrestoDB `TIME`,
which stores milliseconds; the adapter must use `BIGINT` nanoseconds/`VARCHAR`
or document truncation.

Use ordinary Arrow Flight with a custom descriptor, not full Flight SQL, for
the first PoC. Presto already parses and plans SQL. Implementing the Flight SQL
metadata, statement, and transaction surface would duplicate the query engine
without improving pushdown. The [Flight protocol](https://arrow.apache.org/docs/format/Flight.html)
already lets `GetFlightInfo` return many endpoints and `DoGet` stream batches.

### Trino: custom connector required

Trino's SPI is a good production integration point, but Arrow is an internal
detail of the connector. Implement:

- `ConnectorMetadata` for schemas, tables, columns, statistics, and supported
  filter/projection/limit application.
- `ConnectorSplitManager` to turn Sidecar token assignments into splits.
- `ConnectorPageSourceProvider` to call Flight and convert Arrow vectors into
  Trino blocks/pages.
- Correct residual constraints for every predicate the service cannot enforce.

Trino's `Block` interface is sealed, so a plugin cannot insert a new
Arrow-backed block implementation. Java-side vector conversion is therefore
part of the connector cost. Trino's existing
[Cassandra connector](https://trino.io/docs/current/connector/cassandra.html)
and `CassandraTokenSplitManager` are the right planning and performance
baseline, even though they read through Cassandra's native protocol rather than
local SSTables.

Trino explicitly recommends a page source when a connector can create pages
directly, because record-by-record conversion adds overhead. Its
[pushdown documentation](https://trino.io/docs/current/optimizer/pushdown.html)
also makes the performance contract clear: predicate and projection pushdown
must reduce source work and network traffic, not merely remove values after a
full scan.

### Engine-neutral contract

Both integrations should translate into the same service request:

```rust
pub struct SnapshotScanPlan {
    pub table: QualifiedTable,
    pub snapshot_id: String,
    pub schema_digest: [u8; 32],
    pub token_ranges: Vec<TokenRange>, // Cassandra (start, end] semantics
    pub projected_columns: Vec<ColumnId>,
    pub predicates: PredicateExpr,
    pub read_time_micros: i64,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    pub max_value_bytes: usize,
    pub limit_hint: Option<u64>,
}

pub fn stream_arrow(
    manifest: SnapshotManifest,
    plan: SnapshotScanPlan,
) -> impl Stream<Item = Result<arrow::record_batch::RecordBatch>>;
```

`limit_hint` is advisory for distributed scans unless the connector can prove a
global limit. The stock PrestoDB base connector never receives a limit;
populating this field requires custom metadata/table-handle work. Predicate
enforcement must be decided during planning, because a runtime service report
cannot restore a residual predicate. With the stock base connector, all
`TupleDomain` constraints remain residual even if the service also evaluates
them.

## CQLite readiness audit

### Available now

- External SSTable discovery through
  `Database::open_with_discovered_sstables()` in
  [`cqlite-core/src/lib.rs`](../../cqlite-core/src/lib.rs).
- A bounded result channel through `execute_streaming()`, fixed by
  [#790](https://github.com/pmcfadin/cqlite/issues/790).
- Equality, `IN`, `BETWEEN`, and inequality evaluation for simple predicates;
  the recent inequality defect was fixed in
  [#788](https://github.com/pmcfadin/cqlite/issues/788).
- Strong Arrow schema and array coverage for scalars, UUIDs, lists, maps,
  tuples, and UDTs from
  [epic #673](https://github.com/pmcfadin/cqlite/issues/673).
  Important limits remain: fixed-scale `Decimal128`, bounded `varint`,
  `duration` as UTF-8, set/list identity loss, and engine-specific UUID,
  nanosecond-time, and timestamp-timezone mapping.
- Embeddable batch and streaming Parquet writers from
  [epic #682](https://github.com/pmcfadin/cqlite/issues/682).
- A write-side K-way compactor with substantially stronger per-cell
  reconciliation logic in
  [`write_engine/merge.rs`](../../cqlite-core/src/storage/write_engine/merge.rs).

### Blocking gaps

1. **Default reads do not produce a current Cassandra row.** The normal
   multi-SSTable scan concatenates and sorts entries. It does not deduplicate
   keys or apply last-write-wins across generations. A query can therefore
   return duplicates, stale values, or rows deleted in another SSTable.
2. **The optional `tombstones` scan materializes the full result.** It is not a
   bounded streaming reconciler, and its merger is weaker than the write-side
   compactor for arbitrary per-cell conflicts.
3. **The write-side K-way merger is not a read cursor.** It preloads input and
   still loses some authoritative per-cell timestamp information. True cursor
   compaction is tracked by
   [#754](https://github.com/pmcfadin/cqlite/issues/754).
4. **No token-range scan exists.** The deprecated token API delegates to a full
   partition scan. The public scan uses raw key bounds, not Murmur3 `(start,
   end]` bounds.
5. **The cross-SSTable streaming merge orders `RowKey` bytes, not decorated
   Murmur3 keys.** Correct partition grouping and early limits require token,
   then key-byte ordering.
6. **Predicate and projection “pushdown” happen after row decoding.** The
   storage call receives no token or clustering bounds; the executor constructs
   a row and then evaluates predicates. This saves output work, not disk reads.
7. **Residual predicate handling is unsafe.** The optimizer skips `OR` and
   `NOT`, but retains the original filter only when it collected zero pushable
   predicates. A mixed pushable/residual expression can lose part of its
   condition and return incorrect rows.
8. **Projection can remove a predicate column too early.** The current scan row
   contains only selected columns, then predicate evaluation rejects a missing
   `WHERE` column. Internal scan projection must be the union of output,
   predicate, key, and reconciliation columns.
9. **Background scan failures can look like EOF.** `execute_streaming()` logs a
   background error and closes its channel. Flight must surface a terminal
   stream error rather than silently returning a truncated result.
10. **There is no public Arrow batch stream.** Arrow schema and array builders
   are private to the Parquet exporter, and the streaming writer clones rows
   before creating private batches.
11. **Parallel reads share mutable reader position.** A mutex serializes each
   SSTable scan; independent positioned cursors are tracked by
   [#815](https://github.com/pmcfadin/cqlite/issues/815).
12. **Some compressed scans still stitch a complete data section in memory.**
    Memory is bounded by SSTable size rather than batch size.
13. **BTI (`da`) remains unsupported end to end.** The open work is
    [#660](https://github.com/pmcfadin/cqlite/issues/660), with indexed seeks in
    [#755](https://github.com/pmcfadin/cqlite/issues/755).
14. **Counters and Cassandra 5 vectors need explicit policy.** Counter cells
    require `CounterContext` shard merging rather than ordinary LWW. Reject
    counters and vector types in v1 until their reconciliation and Arrow/engine
    mappings are specified and tested.

The existing Parquet position paper says that opening all live SSTables lets
CQLite perform a correct LWW merge. The current default implementation does not
meet that claim. That statement in
[`cassandra-sidecar-parquet-projections.md`](../architecture/cassandra-sidecar-parquet-projections.md)
should be corrected independently.

The open [WRITETIME/TTL epic #689](https://github.com/pmcfadin/cqlite/issues/689)
and [delta-scan epic #696](https://github.com/pmcfadin/cqlite/issues/696) are
useful but do not close the gap. Delta scan deliberately emits one generation
without cross-SSTable reconciliation. The live analytics path needs the
opposite contract: merge all generations into current rows before filtering.

## Recommended CQLite workstream

### A1. Typed snapshot scan plan

Add a structured API for explicit SSTable manifests, Murmur3 token intervals,
clustering bounds, projected columns, typed predicates, reference time, and
batch limits. Keep it below the CQL parser so PrestoDB, Trino, DataFusion, and
tests share one semantic contract.

### A2. Correct streaming reconciliation

Build a cursor-based read-only merger from the strongest logic in the
write-side compactor. It must preserve and compare per-cell timestamps; apply
partition, row, range, and cell tombstones; resolve equal-timestamp delete/live
ties; handle row liveness, static rows, complex-column deletions, and collection
element paths; and evaluate TTL against one query reference time. Cassandra's
full equal-timestamp tie rules are part of the contract.

This is the central project risk. Do not substitute “newest SSTable wins” or
whole-row generation ordering for Cassandra's per-cell semantics.

### A3. Decorated-key token scans

Expose Cassandra-compatible Murmur3 token calculation as part of the scan API.
Represent wrap-around ranges explicitly. Build per-SSTable cursors ordered by
`(token, raw_partition_key)` and group all versions of one partition before
emitting it.

The first correct implementation may scan each SSTable sequentially and discard
unassigned tokens. Later work can seek using BIG indexes and BTI tries. Until
seeks exist, group a node's many vnode intervals so the service scans local
SSTables once rather than once per vnode.

Clustering seeks require more than jumping to the first requested row. The
cursor must reconstruct range tombstones opened before the slice and retain
partition deletions and static rows that affect the requested clustering range.

### A4. Safe predicate and projection pushdown

Separate enforced predicates from residual predicates. Push token and supported
partition/clustering restrictions into the cursor. While walking cells, decode
and allocate only output and predicate columns, plus metadata required for
reconciliation. Evaluate non-key filters only after the live row is assembled.

Fix the mixed `AND`/`OR`/`NOT` residual-loss defect before exposing engine-level
predicate pushdown.

### A5. Public Arrow batch API

Extract CQL-to-Arrow schema and array construction from the Parquet module into
a reusable core module. Return `RecordBatch` values directly and avoid cloning
row `HashMap`s. Preserve schema metadata needed for CQL type identity where
Arrow has no native distinction, such as set versus list and UUID versus binary.

### A6. Positioned, concurrent, bounded readers

Replace shared seek state with per-scan cursors or positioned reads. Parse
compressed chunks incrementally across chunk boundaries. Bound memory by the
number of SSTable cursors, configured Arrow batches, active reconciliation
state, large rows/collections, and overlapping range tombstones. Enforce byte
and value caps plus bounded read-ahead; row-count batching alone is insufficient.

### A7. Format coverage and proof

Complete BTI reading before claiming general Cassandra 5 coverage. Validate the
physical decoder against `sstabledump`, then validate logical reconciliation
against a reference model and a quiesced read from the selected Cassandra
replica at the same TTL reference time. Cover multiple generations, deletes,
TTLs, static columns, collections, equal timestamps, and wrap-around ranges.

## Safe proof-of-concept scope

The work should have two explicit milestones so transport progress does not get
confused with correctness.

### Milestone 0: transport spike

Use a generated BIG/OA fixture known to contain the complete table in exactly
one SSTable, with scalar columns, no deletes, no TTL, and no schema changes.
Expose a `DoGet` stream, read it with an Arrow client, then query it through the
PrestoDB base Flight connector.

This proves Flight descriptors, endpoint planning, type conversion,
backpressure, and packaging. It is not a general Cassandra snapshot reader and
must be labeled accordingly.

### Milestone 1: correct snapshot scan

Use a three-node Cassandra + Sidecar cluster with replication factor three and
multiple SSTable generations. Create one snapshot epoch, assign every canonical
token interval to exactly one read replica, reconcile versions with CQLite,
and compare output with authoritative Cassandra/`sstabledump` expectations.
Use `sstabledump` only for physical-record parity; use the reference reconciler
and a quiesced selected-replica read for current-row parity.

Milestone 1 is the minimum credible demonstration of “fast analytics from
Cassandra.”

## Delivery plan

### Phase 1 — contract and fixtures

- Define `SnapshotManifest`, `SnapshotScanPlan`, predicate capabilities, Arrow
  metadata, errors, and scan metrics. The manifest must bind snapshot epoch,
  table ID/incarnation, generation, data-directory index, complete TOC
  component set, component sizes/checksums, and schema version/digest. Reject
  incomplete or changing manifests.
- Generate deterministic multi-generation fixtures with updates, all tombstone
  shapes, TTL, static rows, collections, and token wrap-around.
- Add a reference reconciliation model and `sstabledump` goldens.

### Phase 2 — CQLite scan kernel

- Implement decorated-key cursors and grouped token interval filtering.
- Implement bounded, per-cell multi-SSTable reconciliation.
- Add safe decode pruning and residual predicate evaluation.
- Expose `stream_arrow()` and batch-level backpressure.
- Run the required CQLite core Clippy gate in addition to tests.

### Phase 3 — standalone Flight service

- Implement schema and scan descriptors, `GetSchema`, `GetFlightInfo`, and
  `DoGet` with the Rust Arrow Flight crate.
- Add signed tickets, mTLS, deadlines, cancellation, resource quotas, and scan
  metrics.
- Support local snapshot mounts first; preserve an interface for Sidecar HTTP
  range storage later.

### Phase 4 — Sidecar orchestration

- Read settings, schema, topology, replica maps, and health.
- Create/reuse snapshot epochs with TTL and explicit cleanup.
- Assign full ring coverage once, group ranges by node, and reject gaps or
  overlaps.
- Handle partial snapshot creation and topology changes.

### Phase 5 — PrestoDB integration

- Extend `presto-base-arrow-flight` with Cassandra schema/table discovery,
  descriptor encoding, credentials, and type overrides.
- Pass desired columns and safe `TupleDomain` constraints to the gateway.
- Keep all unproven constraints residual in Presto.
- Validate `SHOW`, `DESCRIBE`, scans, projection, filters, engine-side limits,
  aggregates, and joins.

### Phase 6 — Trino and production hardening

- Build a Trino connector only if Trino is the deployment target.
- Add cost/statistics reporting, adaptive split sizing, retry policy, audit
  logs, rolling upgrades, and compatibility matrices.
- Benchmark local-volume and Sidecar-range modes under Cassandra production
  load.

## Correctness and performance gates

### Correctness

- Every canonical token interval appears exactly once in a query plan.
- Multi-generation output matches the reference model for inserts, partial
  updates, deletes, TTL, static rows, and collections.
- Unsupported predicates remain residual in the query engine.
- All nodes use one schema digest and TTL reference time.
- Retried tickets stay on the original replica and return rows from the same
  snapshot.
- A node failure produces a clear v1 query failure; it never silently switches
  replicas or drops a range. Cross-replica failover requires explicitly weaker
  consistency semantics in a later version.

### Performance

Measure rather than assume:

- Time to first Arrow batch.
- Rows and decoded bytes per second.
- SSTable bytes read versus bytes returned.
- CPU per decoded and returned row.
- Peak memory and allocation rate.
- Arrow/Flight network bytes and backpressure time.
- Reconciliation fan-in: SSTables and versions per partition.
- Snapshot creation time and pinned disk bytes.
- Cassandra foreground latency while scans run.

The benchmark matrix should vary SSTable count, result selectivity, projected
column fraction, row width, compression, tombstone density, TTL density,
concurrency, and BIG versus BTI. Compare against the query engine's Cassandra
connector and Apache Cassandra Analytics/Spark where practical.

A reasonable PoC go/no-go target is zero correctness differences and a
material scan-heavy speedup over the existing Cassandra connector on the same
hardware. A numeric multiplier should be set only after the baseline harness is
stable. Memory must remain bounded when dataset size increases by an order of
magnitude.

## Failure handling

| Failure | Required behavior |
|---|---|
| Sidecar unavailable during planning | Do not issue a partial ring plan |
| Snapshot succeeds on only some nodes | Mark epoch incomplete and clean it up |
| Topology changes after planning | Finish against the captured snapshot/map or fail; never silently reassign live files |
| Flight node fails | Fail v1 clearly; later failover may use another snapshotted replica only with weaker consistency semantics |
| Schema digest mismatch | Fail before streaming rows |
| Unsupported SSTable format/partitioner | Fail during planning with node and file context |
| Query cancellation/deadline | Stop decode promptly, close streams, release lease |
| Consumer backpressure | Bound batches and pause SSTable parsing |
| Disk pressure | Reject snapshot creation or scans according to policy |
| Scanner or bad predicate/type mapping | Send a terminal Flight error; never turn failure into clean EOF |

## Security and operational controls

Sidecar's project documentation still labels it work in progress; its latest
published release is 0.4.0 as of this document's date. Pin and test one Sidecar
and Cassandra combination before expanding the matrix. Sidecar requires
Cassandra 4.0+ and Java 11+; current adapter families cover Cassandra 4.0, 4.1,
and 5.x. See the [Sidecar repository](https://github.com/apache/cassandra-sidecar)
and [user guide](https://github.com/apache/cassandra-sidecar/blob/trunk/docs/src/user.adoc).

Sidecar's shipped configuration binds HTTP to `0.0.0.0`, with TLS and access
control disabled. Those defaults are unsuitable for raw SSTable access outside
a development network.

Production defaults must include:

- TLS and preferably mTLS on Sidecar and Flight.
- Sidecar RBAC scoped to ring, topology, schema, and snapshot
  create/read/stream/delete. Component streaming also requires
  `SNAPSHOT:STREAM` and Cassandra table `SELECT`. The v1 settings endpoint is
  authenticated but is not authorization-scoped, so network policy remains
  important.
- Query-engine principal propagation and table authorization.
- Signed, short-lived Flight tickets that cannot name arbitrary files.
- Read-only Cassandra volume mounts under a dedicated service user.
- Per-node CPU, memory, I/O, concurrency, batch-size, and result-byte limits.
- Snapshot TTL plus explicit cleanup and disk-pressure admission control.
- Cancellation and deadlines propagated from the query engine.
- Metrics and audit records keyed by query, snapshot, table, node, and token range.

## Risks and open decisions

1. **Correct reconciliation is the largest technical risk.** Existing CQLite
   code provides pieces, but no current path is both fully correct and bounded.
2. **Snapshot cadence defines freshness and cost.** Per-query snapshots are
   expensive; reused snapshots are stale. The product needs an explicit
   freshness objective.
3. **Replica reads are eventually consistent.** The service needs an honest
   consistency statement and an optional verification strategy for stricter
   users.
4. **Row storage limits projection savings.** Narrow queries still read SSTable
   bytes; benchmarks must separate decoding, reconciliation, and transport wins.
5. **BTI coverage affects deployability.** A Cassandra 5 cluster using BTI
   cannot use the service until #660 lands.
6. **PrestoDB versus Trino is an operating decision.** PrestoDB shortens the
   PoC because its base connector exists. Trino may still be the required
   production engine.
7. **Local files versus Sidecar ranges is a deployment decision.** Local mounts
   maximize speed; HTTP ranges reduce filesystem coupling and follow Cassandra
   Analytics precedent.
8. **Sidecar snapshot TTL differs by Cassandra version.** Cassandra 4.0 ignores
   the TTL option, so explicit cleanup is mandatory.

## Recommendation

Proceed, but split the work into a transport spike and a correctness milestone.
Start the sibling project with a per-node Flight service, a minimal gateway,
and a PrestoDB plugin. In parallel, make CQLite's `SnapshotScanPlan`, streaming
reconciler, token-range cursor, and public `RecordBatch` stream the critical
path.

Do not describe the result as a correct Cassandra analytics path until the
multi-generation reconciliation and exactly-once token coverage tests pass.
Once they do, the design has a credible advantage: it reads immutable Cassandra
storage locally, distributes work using the ring Cassandra already maintains,
and hands query engines a columnar network stream without putting analytical
row scans through Cassandra coordinators.

## Primary sources

- [Apache Cassandra Sidecar](https://github.com/apache/cassandra-sidecar)
- [Sidecar API route definitions](https://github.com/apache/cassandra-sidecar/blob/trunk/client-common/src/main/java/org/apache/cassandra/sidecar/common/ApiEndpointsV1.java)
- [Sidecar token-range replica response](https://github.com/apache/cassandra-sidecar/blob/trunk/client-common/src/main/java/org/apache/cassandra/sidecar/common/response/TokenRangeReplicasResponse.java)
- [Sidecar SSTable access module](https://github.com/apache/cassandra-sidecar/blob/trunk/server/src/main/java/org/apache/cassandra/sidecar/modules/SSTablesAccessModule.java)
- [Apache Cassandra Analytics](https://github.com/apache/cassandra-analytics)
- [PrestoDB Arrow Flight connector](https://prestodb.io/docs/current/connector/base-arrow-flight.html)
- [PrestoDB base connector source](https://github.com/prestodb/presto/tree/master/presto-base-arrow-flight)
- [Trino connector SPI](https://trino.io/docs/current/develop/connectors.html)
- [Apache Arrow Flight protocol](https://arrow.apache.org/docs/format/Flight.html)
- [CQLite Arrow type epic #673](https://github.com/pmcfadin/cqlite/issues/673)
- [CQLite embeddable Parquet epic #682](https://github.com/pmcfadin/cqlite/issues/682)
- [CQLite WRITETIME/TTL epic #689](https://github.com/pmcfadin/cqlite/issues/689)
- [CQLite delta-scan epic #696](https://github.com/pmcfadin/cqlite/issues/696)
- [CQLite streaming K-way merge #754](https://github.com/pmcfadin/cqlite/issues/754)
- [CQLite BTI read epic #660](https://github.com/pmcfadin/cqlite/issues/660)
