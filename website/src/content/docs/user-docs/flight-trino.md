---
title: Arrow Flight Server & Trino Connector
description: Query a Cassandra node's SSTables as Arrow streams with the cqlite-flight server, and run distributed SQL over a whole cluster from Trino — compaction-on-read, container image, the client ticket contract, and connector install.
sidebar:
  label: Flight & Trino
  order: 8
---

# Arrow Flight Server & Trino Connector

CQLite ships two components for querying Cassandra SSTables as Arrow data over the
network:

- **`cqlite-flight`** — an [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
  server that runs co-located with a single Cassandra node and exposes that node's
  SSTables as queryable Arrow streams.
- **`cqlite-trino`** — a Trino connector that uses the Cassandra **Sidecar** to
  discover the ring, then fans out one Flight read per token range so a `SELECT`
  in Trino scans a whole cluster's SSTables.

Together they turn a Cassandra cluster's on-disk data into a distributed,
analytics-friendly SQL surface **without a read path through Cassandra's own query
engine**.

This page is a user-facing distillation. The authoritative, exhaustive flag and
property reference lives in the crate READMEs:

- [`cqlite-flight/README.md`](https://github.com/pmcfadin/cqlite/blob/main/cqlite-flight/README.md)
- [`trino-connector/README.md`](https://github.com/pmcfadin/cqlite/blob/main/trino-connector/README.md)
- [`docs/flight-trino/PLAN.md`](https://github.com/pmcfadin/cqlite/blob/main/docs/flight-trino/PLAN.md)
  — full design context.

## The Flight server and its compaction-on-read model

`cqlite-flight` runs **co-located with a Cassandra node** and reads that node's
SSTables directly off disk. On **every** read it performs an on-the-fly
**compaction merge** — a k-way merge of the table's SSTables that resolves
last-write-wins and applies tombstones — then filters and streams the result as
Arrow record batches. Key properties:

- **Compaction on read.** Each `DoGet` runs a full k-way merge of the table's
  SSTables. **The original SSTables are never modified or removed** — the merge
  produces Arrow batches, not new files on disk.
- **Reads flushed SSTables only.** It reads data that Cassandra has already
  flushed to disk. Rows still in a memtable are invisible; run `nodetool flush`
  to make recent writes readable.
- **Snapshot-aware.** When a ticket names a Sidecar snapshot, the server reads
  that hard-linked, immutable snapshot directory instead of the live data dir —
  giving a stable file set even while Cassandra compacts underneath.
- **`SELECT` parity.** Rows are reconstructed with the same read path CQLite's
  query engine uses, so the Arrow output matches what a Cassandra `SELECT` would
  return.

Because it reads on-disk files rather than issuing CQL, it adds no read load to
Cassandra's own request path. For a broader comparison of CQLite's read surfaces
and their freshness guarantees, see
[Read surfaces & freshness](/cqlite/user-docs/read-surfaces-and-freshness/).

## Run the published container

The server publishes as a multi-arch (`linux/amd64` + `linux/arm64`) image to the
GitHub Container Registry:

```
ghcr.io/pmcfadin/cqlite-flight
```

Once the package is public, pulling needs **no authentication** (no `docker
login`):

```bash
docker pull ghcr.io/pmcfadin/cqlite-flight:latest   # or a vX.Y.Z release tag
```

The container ships no data of its own — it reads SSTables from a directory you
**mount at runtime**. Run it co-located with a Cassandra node, mounting that
node's data dir **read-only** (`:ro`) and pointing `--data-dir` at it. It serves
the Arrow Flight gRPC listener on `:8815`:

```bash
docker run --rm -p 8815:8815 \
  -v /var/lib/cassandra:/var/lib/cassandra:ro \
  -e RUST_LOG=info \
  ghcr.io/pmcfadin/cqlite-flight:latest \
  --data-dir /var/lib/cassandra/data --listen 0.0.0.0:8815
```

Notes:

- The **read-only mount** is recommended — the server never writes to the
  SSTables, it merges them on the fly into Arrow batches.
- Any directory laid out as `<keyspace>/<table>[-<uuid>]/` works as `--data-dir`;
  it does not have to be a live node (a snapshot or exported SSTable tree works
  too).
- The image runs as a **non-root** user (`uid 10001`); ensure the mounted path is
  readable by that uid.

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | *(required)* | Root holding `<keyspace>/<table>[-<uuid>]/` SSTable dirs. |
| `--listen` | `0.0.0.0:8815` | Flight gRPC listen address. |
| `--batch-size` | `8192` | Max rows per Arrow record batch. |

See the [server README](https://github.com/pmcfadin/cqlite/blob/main/cqlite-flight/README.md#container-image-ghcr)
for the full tag scheme, running from source, and building the image yourself.

## The client ticket contract

Clients address a table with a JSON **ticket**. The server exposes a **read-only**
Arrow Flight RPC surface:

| RPC | Purpose |
|-----|---------|
| `GetFlightInfo` | Returns the Arrow schema (derived from the ticket DDL) plus an endpoint/ticket. |
| `GetSchema` | Returns just the Arrow schema for a ticket. |
| `DoGet` | Streams the merged, filtered table as Arrow record batches. |
| `DoAction("table_stats")` / `ListActions` | Returns per-table aggregate statistics (Σ live rows, Σ partition count, SSTable count) that the Trino connector uses to drive aggregation-pushdown planning. |

The only supported action is `table_stats`; every other action type is
`unimplemented`. All write/exchange RPCs (`DoPut`, `DoExchange`, `Handshake`, …)
are likewise unimplemented — it is a read-only server.

The ticket is a small JSON document. Required fields name the table and carry the
DDL used to build the schema for the merge; the rest are optional pushdowns:

```jsonc
{
  "keyspace": "ks",
  "table": "tbl",
  "ddl": "CREATE TABLE ks.tbl (...)",  // parsed into the schema for the merge
  "snapshot": "cqlite-abc",            // optional; null = live data dir
  "token_start": -3074457345618258602, // optional, exclusive  (range is (start, end])
  "token_end":   3074457345618258602,  // optional, inclusive
  "wraparound": false,                 // optional; true when token_start > token_end (range wraps the ring)
  "columns": ["pk", "v"],              // optional projection; null = all columns
  "predicates": [                      // optional; AND-combined, evaluated per row
    { "column": "v", "op": "Gt", "value": 10 }
  ]
}
```

- **`columns`** is a projection — only the listed columns are read and streamed.
  The Arrow schema reflects the projection.
- **`predicates`** are AND-combined and evaluated per row. `op` is one of
  `Equal`, `In` (value is a JSON array), `Gt`, `Gte`, `Lt`, `Lte`, `Prefix`.
  Operands are typed by the column's CQL type.
- **`token_start` / `token_end`** filter on the token already stored on each
  partition key — no Murmur3 computation. This is how the Trino connector assigns
  a token range to each split. Set **`wraparound`** to `true` for a range that
  crosses the ring's minimum token (`token_start > token_end`); it defaults to
  `false`.

### PyArrow Flight example

```python
import json
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")

ticket_json = json.dumps({
    "keyspace": "test_basic",
    "table": "simple_table",
    "ddl": "CREATE TABLE test_basic.simple_table (id uuid PRIMARY KEY, name text)",
    # optional pushdowns:
    # "columns": ["id", "name"],
    # "predicates": [{"column": "name", "op": "Prefix", "value": "a"}],
}).encode()

# Fetch just the Arrow schema:
info = client.get_flight_info(flight.FlightDescriptor.for_command(ticket_json))
print(info.schema)

# Stream the merged, filtered table:
reader = client.do_get(flight.Ticket(ticket_json))
table = reader.read_all()
print(table.to_pandas())
```

`uuid` / `timeuuid` columns carry the Arrow UUID extension metadata. See the
[ticket contract in the README](https://github.com/pmcfadin/cqlite/blob/main/cqlite-flight/README.md#flight-ticket-contract)
for the complete field reference.

## The Trino connector

`cqlite-trino` is a Trino plugin that turns the single-node Flight server into a
**cluster-wide** SQL surface. A `SELECT` over `cqlite.<keyspace>.<table>` streams
compaction-merged, token-range-deduped Arrow data back to Trino.

### Discovery and the split model

The connector discovers nodes and token ranges through the Cassandra **Sidecar**
(ring, token-range-to-replica mapping, and schema). It then builds **one split
per token range, pinned to a single replica** — so each row is read exactly once
cluster-wide, with no duplication across replicas. Each split issues a `DoGet` to
its replica's `cqlite-flight` endpoint with that token range in the ticket.

The split count follows the Sidecar's token-range response for the ring — roughly
`nodes × vnodes_per_node` (e.g. a single node with Cassandra's default 16 vnodes
yields 16 splits; an N-node vnode ring scales up accordingly).

**v1 type support:** scalar columns (int, bigint, text, boolean, uuid, timestamp,
…). Complex CQL types (collections, UDTs, tuples, decimal) are rejected at
planning with a clear message rather than failing mid-scan.

### Install the plugin

A Trino plugin is **not a single jar** — Trino loads each plugin from an isolated
classloader directory, so the connector jar must sit alongside all of its runtime
dependencies. You assemble that *directory of jars* and drop it into Trino's
plugin path (`/usr/lib/trino/plugin/cqlite_flight`).

**From this repo** (the Gradle wrapper lives in `trino-connector/`):

```bash
cd trino-connector && ./gradlew installPlugin   # produces build/plugin/cqlite_flight/
```

**From the published Maven Central artifact** (`in.mcfad:cqlite-trino:<version>`),
assemble the directory with a throwaway Gradle project that copies the resolved
runtime classpath:

```kotlin
// build.gradle.kts
plugins { java }
repositories { mavenCentral() }
dependencies { implementation("in.mcfad:cqlite-trino:<version>") }
tasks.register<Sync>("assemblePlugin") {
    into(layout.buildDirectory.dir("plugin/cqlite_flight"))
    from(configurations.runtimeClasspath)
}
```

`./gradlew assemblePlugin` yields `build/plugin/cqlite_flight/` (connector jar +
flight-core + jackson + transitive deps). `trino-spi` is intentionally **not** a
runtime dependency — Trino supplies it from the engine classpath.

### Required JVM configuration (Arrow off-heap, JDK 17+)

The connector reads Flight batches into off-heap Apache Arrow buffers. On JDK 17+
arrow-java's `MemoryUtil` initializer needs the `java.nio` module opened to it, so
Trino's `jvm.config` **must** contain this exact line:

```
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

Trino 481's stock `jvm.config` does not include it. Without the flag the first
Arrow off-heap touch fails in `MemoryUtil.<clinit>`, and every `do_get` then dies
downstream with an opaque `Failed to read message`. Add the flag to **every**
coordinator and worker, then restart.

The connector fails fast to make this actionable: it probes Arrow memory
initialization once at catalog load and, if the flag is missing, raises a Trino
`CONFIGURATION_INVALID` error naming this exact flag and the `jvm.config` remedy —
rather than surfacing the cryptic read-time error. The bundled docker stack and
the k8s `trino-cqlite` kit both configure the flag for you.

### Catalog configuration and read mode

Configure a catalog in `etc/catalog/cqlite.properties`. The first line must select
the connector factory by name; the rest are the `cqlite.*` properties:

```properties
# etc/catalog/cqlite.properties
connector.name=cqlite_flight
cqlite.sidecar-uri=http://cassandra-sidecar:9043
# cqlite.read-mode=snapshot   # optional; snapshot is the default
```

The key properties:

| Property | Default | Description |
|----------|---------|-------------|
| `connector.name` | *(required)* | Must be `cqlite_flight` — selects this connector's factory so Trino can load the catalog. |
| `cqlite.sidecar-uri` | *(required)* | Cassandra Sidecar base URI for DDL / ring discovery. |
| `cqlite.flight-port` | `8815` | Arrow Flight port on each Cassandra node. |
| `cqlite.local-datacenter` | *(none)* | Preferred datacenter for split placement. |
| `cqlite.read-mode` | `snapshot` | See below. |

The **`cqlite.read-mode`** property picks which file set a scan sees, since
Cassandra compacts and flushes continuously underneath:

| Mode | File set | Consistency tradeoff |
|------|----------|----------------------|
| `snapshot` (default) | A **Cassandra Sidecar snapshot** — a hard-linked, immutable copy of the SSTables taken at planning time. | **Stable file set, bounded staleness.** Every split reads the same immutable files; data is "as of" snapshot time, with no mid-scan file churn. The safe default. |
| `live` | The node's current data directory. | **Most current, but races compaction.** A long scan can have files compacted or removed under it. Use when you want the absolute latest flushed data and accept the race. |

In `snapshot` mode the connector creates a per-query snapshot (`cqlite-<queryId>`)
at planning time, names it in every ticket, and best-effort deletes it when the
query finishes; a TTL backstop (`cqlite.snapshot-ttl`, default `6h`) ensures a
coordinator crash can't leak it. Snapshot creation is **fail-closed** — if it
fails, the query fails rather than silently falling back to a live read.

**Memtable visibility differs by mode (issue #2305).** `snapshot` mode is a per-query,
point-in-time read that **includes durable memtable state**: Cassandra's snapshot creation
flushes the memtable as a side effect by design, and the Sidecar's create-snapshot HTTP API has
no `skipFlush` parameter to opt out of it. So a very recent write can become visible through a
`snapshot`-mode query even without an explicit `nodetool flush`. `live` mode is the strict
**on-disk-SSTables-only** contract: it reads the current data directory directly, with no
snapshot and no flush side effect, so memtable rows stay invisible until an explicit
`nodetool flush`. See the
[connector README](https://github.com/pmcfadin/cqlite/blob/main/trino-connector/README.md#read-mode-snapshot-vs-live)
for the full mechanism.

See the [connector README](https://github.com/pmcfadin/cqlite/blob/main/trino-connector/README.md#catalog-configuration)
for the complete property reference, the docker-compose end-to-end topology,
LIMIT / aggregation pushdown behavior, and version-skew notes.

## Where this fits

The Flight server + Trino connector are one of several CQLite read surfaces. For
building a Parquet lakehouse projection triggered by SSTable flush events instead,
see [Lakehouse projections with the Cassandra Sidecar](/cqlite/user-docs/use-cases/sidecar-lakehouse/).
