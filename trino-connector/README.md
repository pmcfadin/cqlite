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
