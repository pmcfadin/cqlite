# cqlite-trino

A Trino connector that queries Apache Cassandra **directly from SSTables** via the
[`cqlite-flight`](../cqlite-flight) Arrow Flight server. It discovers nodes and
token ranges through the Cassandra **Sidecar**, builds one split per token range
pinned to a single replica (so each row is read once cluster-wide), and streams
compacted, filtered data back as Arrow.

- **Java 25**, Trino SPI 481, Arrow Flight Java 18.1 (Gradle wrapper 9.1 +
  foojay auto-provisions the JDK 25 toolchain — no host JDK 25 needed).
- Build & test: `./gradlew test`
- Assemble the Trino plugin dir: `./gradlew installPlugin` → `build/plugin/cqlite_flight`

## Maven Central / published artifact

The connector is published to Maven Central on every `v*` release tag:

```
in.mcfad:cqlite-trino:<version>
```

A Trino plugin is **not a single jar** — Trino loads each plugin from its own
isolated classloader directory, so the connector jar must sit alongside all of
its runtime dependencies. The published Maven artifact is just the connector jar;
to install it you assemble that *directory of jars* (exactly what `./gradlew
installPlugin` produces under `build/plugin/cqlite_flight/`), then drop the
directory into Trino's plugin path (`/usr/lib/trino/plugin/cqlite_flight`).

Assemble the plugin directory from the published artifact with a throwaway
Gradle/Maven project that depends on the coordinates above and copies the
resolved runtime classpath into one directory, e.g.:

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
flight-core + jackson + transitive deps). Note `trino-spi` is intentionally
**not** a runtime dependency — Trino supplies it from the engine classpath.
Building from this repo (`./gradlew installPlugin`) produces the same directory.

## Status

| Piece | State |
|---|---|
| Sidecar client (ring / token-range-replicas / schema) | done, tested |
| Arrow→Trino type mapping | done, tested |
| Plugin / ConnectorFactory / Connector / Config | done |
| SplitManager (one split per range → one replica) | done, tested |
| docker-compose E2E topology | done, **E2E passing** |
| Metadata column resolution (GetSchema→Arrow→Trino) | done |
| PageSource (Flight DoGet → Arrow → Trino Page) | done |
| Projection pushdown (only requested columns streamed) | done |
| Predicate pushdown (TupleDomain → ticket) | deferred (Trino post-filters; server supports it) |
| LIMIT pushdown (per-split bounded scan) | done, tested (issue #2129) |

The full stack works end-to-end: `SELECT` over `cqlite.<keyspace>.<table>`
streams compaction-merged, token-range-deduped Arrow data back to Trino.
Validated types include int/bigint/text/boolean/uuid/timestamp/date/time.

**v1 type support:** scalar columns, including CQL `time` (mapped to Trino
`TIME(9)`, nanosecond precision). Complex CQL types (collections, UDTs, tuples,
decimal, varint) are not yet materializable.

**Per-column degradation (issue #2229):** a column of an unsupported type no
longer makes the whole table unqueryable. Such columns are *hidden* from the
Trino schema — omitted from both the column handles and `DESCRIBE` — and a single
warning per table names the hidden columns and their Arrow type. `SELECT *` and
`SELECT`ing any supported column work normally; hidden columns cannot be
referenced. If *every* column of a table is unsupported the table is genuinely
unqueryable, so it fails fast with a clear `NOT_SUPPORTED` error rather than
presenting a confusing zero-column table.

Cassandra's default 16 vnodes produce 16 splits per table (each reads the SSTable
filtered by token range) — correct, with split consolidation a future
optimization.

### LIMIT pushdown (issue #2129)

`SELECT ... LIMIT k` pushes the row cap into the Flight ticket via
`ConnectorMetadata.applyLimit`, so each split's `do_get` stops its k-way
compaction-merge after `k` rows instead of scanning its entire token range.
Without this, even `SELECT * ... LIMIT 5` full-scans and merges every SSTable
(the reported ~235s over ~2M rows); with it, `LIMIT k` is bounded per-split work.

Semantics:

- **Per-split cap.** Each of the (token-range) splits independently caps at `k`
  rows, so the union returned to Trino can exceed `k`. The connector therefore
  returns `limitGuaranteed = false` and Trino keeps its `Limit`/`LimitPartial`
  above the `TableScan` to do the final global cut — always correct.
- **Counted after filtering.** The server applies the cap AFTER token pruning and
  predicate filtering, so a filtered scan returns as many matching rows as exist
  up to `k`, never fewer.
- **Not pushed under aggregation.** An aggregated handle (`count`/`sum`/… with or
  without `GROUP BY`) already collapses the row set; a row `LIMIT` there is
  meaningless, so `applyLimit` declines and the server ignores `limit` if a ticket
  ever carries both.

**Version requirements.** LIMIT pushdown needs BOTH a connector and a
cqlite-flight server that understand the ticket's `limit` field (connector +
flight ≥ the release carrying #2129). The field is additive-optional and the
Rust ticket is not `deny_unknown_fields`, so version skew is safe in either
direction: an older server silently ignores `limit` (correct full scan, just
unbounded), and an older connector never sets it (server defaults to no cap).
Skew only forfeits the speedup — it never returns wrong rows.

## End-to-end stack (docker)

See [`docker/docker-compose.yml`](docker/docker-compose.yml). Topology:

- **cassandra** (`cassandra:5.0`) on a custom bridge `scylla_rust_driver_public`
  (`172.42.0.0/16`), bound to its network IP `172.42.0.2` (not 127.0.0.1).
- **sidecar** (`ghcr.io/apache/cassandra-sidecar:latest`) shares Cassandra's
  network namespace → **same IP** (`172.42.0.2`), serving on `:9043`.
- **cqlite-flight** co-located (shares Cassandra's IP), reads the local SSTable
  volume, serves Arrow Flight on `:8815`.
- **trino** (`trinodb/trino:481`) with this connector plugin mounted.

```bash
./gradlew installPlugin
cd docker && docker compose up --build
# in another shell:
docker compose exec trino trino
trino> SELECT * FROM cqlite.<keyspace>.<table> LIMIT 10;
```

## Catalog configuration

Catalog properties (`etc/catalog/cqlite.properties`):

| Property | Default | Description |
|----------|---------|-------------|
| `cqlite.sidecar-uri` | *(required)* | Cassandra Sidecar base URI for DDL/ring discovery. |
| `cqlite.flight-port` | `8815` | Arrow Flight port on each Cassandra node. |
| `cqlite.local-datacenter` | *(none)* | Preferred datacenter for split placement. |
| `cqlite.read-mode` | `snapshot` | `snapshot` (default) reads a consistent, per-query Sidecar snapshot; `live` reads the current data dir (races compaction). See [Read mode](#read-mode-snapshot-vs-live). |
| `cqlite.snapshot-ttl` | `6h` | `snapshot` mode only: a Cassandra 4.1+ TTL on each per-query snapshot so a coordinator crash between create and cleanup can't leak it (Cassandra auto-drops it). Set blank to disable. |
| `cqlite.aggregation-pushdown-group-by` | `automatic` | GROUP BY aggregation-pushdown policy: `automatic`, `always`, or `never`. Global aggregates (no GROUP BY) are an unconditional win and always push regardless of this setting. |
| `cqlite.aggregation-pushdown-max-group-ratio` | `0.5` | `automatic` only: decline GROUP BY pushdown once the estimated distinct-groups / rows ratio exceeds this. Must be in `(0.0, 1.0]`. |

**Aggregation-pushdown gate (issue #893).** The single-finalize-split design wins
big for global aggregates and low/mid-cardinality `GROUP BY`, but degrades to
break-even (and slightly negative on bytes) once the number of distinct groups
approaches the row count. `automatic` declines pushdown in that high-cardinality
case so Trino aggregates locally — but only when a cardinality estimate is
available. No authoritative per-grouping-column NDV is surfaced through the
Flight/Sidecar path yet (Cassandra's `Statistics.db` does not store it), so today
`automatic` always pushes. Operators who hit the high-cardinality loss can force
the gate with `cqlite.aggregation-pushdown-group-by=never`; `always` restores the
unconditional pre-gate behavior.

## Read mode (snapshot vs live)

`cqlite-flight` compaction-merges a node's SSTables on **every** read, and Cassandra
compacts and flushes those files continuously underneath. `cqlite.read-mode` picks which
file set a scan sees:

| Mode | File set | Consistency | Use when |
|------|----------|-------------|----------|
| `snapshot` (default) | A **Cassandra Sidecar snapshot** — a hard-linked, immutable copy of the SSTable set taken at planning time. | **Stable file set, bounded staleness.** Every split of the query reads the same immutable files; data is "as of" snapshot time. No mid-scan file churn. | You want a consistent, repeatable read (the safe default). |
| `live` | The node's current data directory. | **Most current, but races compaction.** A long scan can have files compacted/removed under it. | You are stress-hunting the read path, or want the absolute latest flushed data and accept the race. |

Both modes only ever see **flushed** SSTables — memtable rows are invisible until a
`nodetool flush` (a property of the SSTable-based server, not of read mode).

### Snapshot lifecycle (per-query, `cqlite-<queryId>`)

In `snapshot` mode the connector, at **split-planning time** (once per query, per table):

1. Creates a Sidecar snapshot named `cqlite-<queryId>` of the scanned table
   (`PUT /api/v1/keyspaces/{keyspace}/tables/{table}/snapshots/{name}`, with the
   configured `?ttl=` backstop).
2. Names that snapshot in **every** Flight ticket, so all splits read the one immutable
   file set.
3. Best-effort **deletes** the snapshot when the query finishes — success or failure —
   via Trino's `cleanupQuery` hook
   (`DELETE /api/v1/keyspaces/{keyspace}/tables/{table}/snapshots/{name}`).

Why per-query rather than a long-lived reusable snapshot: it needs no background reaper,
each query gets an isolated consistent view, and the queryId makes the name collision-free
and traceable. The `cqlite.snapshot-ttl` backstop means even a coordinator crash between
create and cleanup can't leak the snapshot — Cassandra auto-drops it.

**Fail-closed:** if snapshot creation fails in `snapshot` mode, the query **fails** — the
connector does **not** silently fall back to a live read. Falling back would hand back a
compaction-racing result the operator explicitly asked to avoid. Switch to
`cqlite.read-mode=live` to opt into that behavior deliberately.

The Sidecar endpoint paths and HTTP verbs above match apache/cassandra-sidecar
`ApiEndpointsV1.SNAPSHOTS_ROUTE` (`CreateSnapshotRequest` → `PUT`, `ClearSnapshotRequest`
→ `DELETE`).

**Statistics use the live dir, even in `snapshot` mode.** The optimizer's `table_stats`
fetch runs during query *planning*, which is earlier than split planning — so the per-query
snapshot does not exist yet when the row-count estimate is gathered. The connector therefore
estimates off the current data directory (`snapshot=null` on the `table_stats` request). The
scan itself still reads the immutable snapshot; only the row-count *estimate* comes from the
live dir. This is a plan-quality concern (the optimizer may pick a slightly different
join/aggregation strategy), never a correctness one — returned rows always come from the
snapshot. We deliberately do not create the snapshot earlier just to tighten this estimate.

## Load testing (optional `loadtest` profile)

`cassandra-easy-stress` is wired in behind a separate `loadtest` profile, so a
plain `docker compose up` ignores it. Run it on demand:

```bash
# default KeyValue workload (1m, 50% reads)
docker compose --profile loadtest up cassandra-easy-stress

# or a custom one-off run
docker compose --profile loadtest run --rm cassandra-easy-stress \
  run BasicTimeSeries --host 172.42.0.2 --dc dc1 -d 5m -r 0.2

# then make the generated data visible to Trino (connector reads flushed SSTables)
docker compose exec cassandra nodetool flush loadtest
docker compose exec trino trino --execute "SELECT count(*) FROM cqlite.loadtest.keyvalue"
```

It connects over the shared bridge to Cassandra (`172.42.0.2:9042`, datacenter `dc1`).
