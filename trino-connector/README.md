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

The full stack works end-to-end: `SELECT` over `cqlite.<keyspace>.<table>`
streams compaction-merged, token-range-deduped Arrow data back to Trino.
Validated types include int/bigint/text/boolean/uuid/timestamp.

**v1 type support:** scalar columns. Complex CQL types (collections, UDTs,
tuples, decimal) are rejected at planning with a clear message rather than
failing mid-scan. Cassandra's default 16 vnodes produce 16 splits per table
(each reads the SSTable filtered by token range) — correct, with split
consolidation a future optimization.

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
