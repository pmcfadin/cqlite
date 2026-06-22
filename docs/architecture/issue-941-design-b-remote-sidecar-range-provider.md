# Issue 941 Design B: Remote Sidecar Range DataFusion Provider

**Status:** Secondary design for deployments without local Cassandra volume access
**Related overview:** [Issue 941 council analysis](issue-941-datafusion-table-provider-council.md)
**Core idea:** A DataFusion provider reads Cassandra snapshot components through Apache Cassandra Sidecar HTTP range APIs instead of calling node-local `cqlite-flight`.

## Executive summary

This design removes the requirement to deploy `cqlite-flight` beside every Cassandra process. DataFusion execution partitions use Sidecar to create snapshots, list components, and stream SSTable component byte ranges. CQLite reads those remote bytes through a bounded positioned-file abstraction.

```text
DataFusion Session
  -> CqliteSidecarRangeTableProvider::scan(...)
  -> Sidecar ring + token-range replicas + snapshot epoch
  -> CqliteSidecarRangeExec partitions
  -> Sidecar component listing
  -> HTTP range reads for Data.db, Index.db, Summary.db, CompressionInfo.db, Statistics.db
  -> CQLite remote file reader
  -> RecordBatch stream
```

This is less efficient than co-located local reads because the Sidecar and network become the data plane. It is valuable when analytics workers cannot mount Cassandra data directories, especially in Kubernetes or managed environments.

## When to use it

Use this design when:

- Cassandra data volumes cannot be mounted read-only by an analytics sidecar;
- running another process on Cassandra nodes is not allowed;
- Sidecar is already the approved file-access boundary;
- query freshness matters more than the lower cost of materialized epochs;
- the deployment can budget Sidecar and network throughput explicitly.

Do not use this design as the default for high-throughput Trino live scans when co-location is possible. It adds extra copies and makes every indexed seek a remote request.

## Architecture

### Provider

`CqliteSidecarRangeTableProvider` has the same planning responsibilities as Design A:

- fetch schema;
- acquire snapshot epoch;
- plan exactly-once token coverage;
- translate exact predicates;
- build DataFusion execution partitions.

The difference is the execution partition target. Instead of a Flight endpoint, each partition contains:

- Sidecar base URI for the selected replica;
- keyspace/table/table UUID;
- snapshot name;
- token intervals;
- component manifest;
- data directory index;
- request budgets and deadlines.

### Remote file layer

CQLite needs a trait that can satisfy positioned reads without assuming a local `std::fs::File`.

Conceptually:

```rust
trait PositionedReadAt: Send + Sync {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes>;
    async fn len(&self) -> Result<u64>;
}
```

`SidecarComponentFile` implements this trait by issuing HTTP range requests to Sidecar component routes:

```text
GET /api/v1/keyspaces/:ks/tables/:table/snapshots/:snapshot/components/:component
Range: bytes=<start>-<end>
```

The implementation must coalesce small reads, cap concurrency, validate lengths, and cache metadata components within a query.

### Component manifest

Before scanning, the provider builds a manifest per selected node:

- table directory identity, including table UUID/incarnation;
- `dataDirectoryIndex`;
- component names;
- component sizes;
- checksums or digests where available;
- `TOC.txt` membership;
- Summary/Index/CompressionInfo/Statistics presence;
- SSTable format and generation;
- first/last token if derived;
- snapshot creation time.

The scanner must never mix components from different snapshot names or table incarnations.

### Scan execution

For each execution partition:

1. Open remote component handles.
2. Read Summary/Index/CompressionInfo metadata into a bounded cache.
3. Determine SSTable token spans if available.
4. Prune SSTables that cannot overlap the assigned token intervals.
5. Use CQLite's reader and merge logic over remote `read_at` handles.
6. Reconcile rows.
7. Apply exact pushed predicates.
8. Build Arrow batches with row and byte caps.
9. Yield batches through DataFusion `RecordBatchStream`.

## Trino MPP interaction

This design is not a good reason to run DataFusion inside Trino. There are two sane Trino integration options:

1. Keep the Java Trino connector and add a remote Sidecar data path in Java. This is a large rewrite because Java would need equivalent SSTable readers or a Rust service.
2. Run a Rust query-side service that exposes Arrow Flight to Trino, where that Rust service reads Sidecar ranges remotely. Trino still sees Flight streams and Trino pages.

The second option preserves the current Trino connector shape:

```text
Trino worker
  -> Flight DoGet to Rust range-scan service
  -> Rust service reads Sidecar ranges from selected Cassandra replicas
  -> Arrow stream back to Trino
```

That is operationally more complex than Design A and slower than node-local Flight, but it keeps Trino out of the remote SSTable parsing business.

## Massive cluster scheduling

The same exactly-once token coverage rules apply:

- use Sidecar `readReplicas`;
- pick one healthy replica per `(start, end]`;
- fail on gaps or overlaps;
- group adjacent intervals by selected replica;
- do not switch replicas mid-query.

Remote reads add two more constraints:

- per-Sidecar HTTP concurrency must be capped;
- per-query bytes served by each Sidecar must be budgeted separately from Cassandra foreground traffic.

Recommended starting budgets:

- 8-32 in-flight range requests per selected Sidecar;
- 1-4 MiB range request size after coalescing;
- 32-128 MiB per-query metadata cache per worker;
- 50-100 MiB/s max Sidecar egress per Cassandra node, adjusted to cluster capacity;
- retry only idempotent reads against the same component URI and same snapshot manifest.

## Performance model

The local read model becomes:

```text
scan_time ~= max(
  sidecar_range_bytes / sidecar_egress_budget,
  range_request_count * request_latency,
  decompressed_bytes / cpu_decompress,
  decoded_cells * decode_cost,
  merge_events * log2(sstable_fan_in),
  arrow_bytes / consumer_network_budget
)
```

The new term `range_request_count * request_latency` is the danger. Index-driven scans can become a large number of small remote reads. The design must aggressively coalesce reads around compressed chunks and index pages.

Bad pattern:

```text
read 128 bytes from Index.db
read 64 KiB from Data.db
read 128 bytes from Index.db
read 64 KiB from Data.db
...
```

Better pattern:

```text
prefetch Index/Summary windows
coalesce Data.db chunk ranges
read full compression chunks
cache CompressionInfo
stream decoded rows
```

For full-table analytics, the provider should prefer large sequential component ranges and avoid index seeks unless token pruning is selective.

## Predicate and projection pushdown

Remote range reads do not change semantic ordering:

- token and SSTable-span pruning can happen before reconciliation;
- non-key predicates run after live row assembly;
- projection does not remove predicate-only columns from internal reads;
- unsupported predicates remain residual in DataFusion or Trino.

The provider can use DataFusion exact pushdown for the same expression subset as Design A. If expression translation is uncertain, return unsupported.

Projection helps less than it would on a columnar format because Cassandra SSTables are row-oriented. It still avoids allocating, Arrow-converting, and shipping unneeded values after row decode.

## Consistency contract

V1 contract:

```text
Best-effort read-only analytics snapshot assembled from exactly one selected
healthy read replica per canonical token interval over a bounded snapshot
capture window, read through Sidecar component APIs.
```

Additional remote-read requirements:

- every range read must bind to the same snapshot manifest;
- retries must request the same component name, size, and offset;
- if component size changes, fail the query;
- if Sidecar returns an unexpected component list, fail the query;
- if a selected node loses the snapshot mid-query, fail the query.

## Failure handling

| Failure | Behavior |
|---|---|
| range read timeout | retry same component/range within deadline |
| repeated timeout | fail query and release snapshot lease |
| HTTP 404 for component in manifest | fail query; manifest is invalid or snapshot was removed |
| component size mismatch | fail query |
| selected replica down after epoch published | fail query |
| Sidecar throttles requests | back off within query deadline; expose throttling metric |
| client cancellation | cancel outstanding HTTP requests and release lease |

Do not retry on another replica unless the query was planned with an explicit multi-replica fallback contract. V1 should not do that.

## Security

This design routes actual data through Sidecar, so Sidecar authorization and throttling become central:

- authenticate provider to Sidecar;
- restrict keyspaces/tables by caller identity;
- use snapshot names scoped to query IDs;
- log component access by query ID;
- enforce per-caller and per-node byte budgets;
- never expose arbitrary filesystem paths to tickets.

## Required CQLite changes

Design B needs deeper CQLite I/O changes than Design A:

1. Abstract local filesystem reads behind positioned async traits.
2. Teach SSTable component readers to work over `read_at`, not only paths.
3. Make compressed chunk reads coalesce remote ranges.
4. Make Summary/Index/Statistics readers cache-friendly.
5. Stream merge output with cancellation.
6. Add manifest validation before opening readers.

If these abstractions are built well, Design A can also use them for better local positioned reads.

## Validation

Correctness:

- same result as local snapshot scan over identical components;
- retries never change snapshot identity;
- missing component fails;
- changed size fails;
- token coverage remains exact;
- residual filters remain correct.

Performance:

- full scan uses large sequential ranges;
- selective token scan has bounded range request amplification;
- request coalescing reduces small requests;
- metadata cache hit rates are visible;
- Sidecar throttling surfaces cleanly to the query.

Operational:

- snapshot cleanup works after cancellation;
- Sidecar egress budget protects Cassandra;
- provider handles node restart by failing, not hanging;
- metrics distinguish network, Sidecar, CQLite decode, and DataFusion execution time.

## Pros and cons

Pros:

- no node-local `cqlite-flight` deployment;
- uses Sidecar as the approved file-access plane;
- works when Cassandra volumes are inaccessible;
- can run from remote analytics workers.

Cons:

- slower than local disk reads;
- Sidecar becomes part of the hot data path;
- more small-read performance risk;
- requires significant CQLite I/O abstraction work;
- Trino integration still needs Flight or a Java SSTable reader path.

## Recommendation

Keep this as a compatibility design, not the default. It is worth building if the target deployments cannot run co-located readers, but it should follow Design A unless local volume access is impossible.
