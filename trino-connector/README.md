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
| docker-compose E2E topology | done (compose validates) |
| Metadata column resolution (GetSchema→Arrow→Trino) | **TODO (Phase 6)** |
| PageSource (Flight DoGet → Arrow → Trino Page) | **TODO (Phase 6)** |
| Constraint/projection pushdown (TupleDomain → ticket) | **TODO (Phase 6)** |

The Rust `cqlite-flight` server (compaction-merge → token/predicate/projection
filter → Arrow, snapshot-aware) is complete and tested. The connector's
discovery, typing, and split planning are done; the page source and metadata
column wiring are the remaining functional pieces before the E2E can serve queries.

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
