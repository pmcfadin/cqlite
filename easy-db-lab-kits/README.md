# easy-db-lab kits (CQLite)

Out-of-tree [`easy-db-lab`](https://github.com/pmcfadin/easy-db-lab) kits for
standing up a repeatable, **multi-node** load-testing harness for the
`cqlite-flight` Arrow Flight data plane and the `cqlite-trino` connector on a
real Cassandra 5.0 cluster with real observability (VictoriaMetrics, Tempo,
Grafana, Pyroscope). Built for epic
[#2103](https://github.com/pmcfadin/cqlite/issues/2103) — see that issue for
the design decisions and gap analysis behind these kits.

These kits are **consumed, not built here**: `cqlite-flight` deploys the
published `ghcr.io/pmcfadin/cqlite-flight` container image, and `trino-cqlite`
installs the published `in.mcfad:cqlite-trino` Maven artifact. Nothing in this
directory forks or vendors either.

## The four kits

| Kit | Type | What it does |
|---|---|---|
| [`cqlite-flight`](cqlite-flight/README.md) | `db` (DaemonSet) | One Arrow Flight gRPC server pod per Cassandra node, reading that node's local SSTables read-only. The data plane. |
| [`trino-cqlite`](trino-cqlite/README.md.template) | `app` (overlay) | Loads the `cqlite-trino` connector plugin into an already-running `trino` kit and registers the `cqlite` catalog. Installs as `cqlite` (see its README's "Install order" and the naming note in the test plan's Phase 0.2). |
| [`trino-loadtest`](trino-loadtest/README.md) | `app` (bench, `kit-ref`) | Concurrent JDBC read-load driver against a running `trino` kit's `cqlite` catalog. Installs as `trino-loadtest-<target>`. |
| `cqlite-flight/dashboards/cqlite-flight.json` | Grafana dashboard | Auto-installed by the `cqlite-flight` kit's own `start` (kit contract: any `.json` under `dashboards/` is picked up automatically). RPC request rate, latency percentiles, rows/bytes streamed, in-flight requests, and error rate, all filterable by `$cluster`/`$service_name` with data links into Tempo traces. |

The connector's own **read-mode** feature (`cqlite.read-mode=snapshot\|live`,
issue #2105) is not a kit — it's a `cqlite-trino` connector config property.
See `trino-connector/README.md` "Read mode (snapshot vs live)" and the test
plan below for how to exercise it.

## Test plans

- [`test-plans/cqlite-flight-loadtest-3node.md`](test-plans/cqlite-flight-loadtest-3node.md)
  — the full write → flush → read → observe → triage recipe: provision a
  3-node cluster, install all three kits, drive writes with
  `cassandra-easy-stress`, flush, read through Trino in both `snapshot` and
  `live` mode (the latter deliberately racing a concurrent write load against
  compaction), confirm the observability wiring, and triage any anomaly into a
  filed `bug`+`performance` issue.

## Installing out-of-tree

Register this directory as an `easy-db-lab` kit source once per cluster
workspace, then install by the kit's own directory name (this wires each
kit's declared `args:` correctly — `--from` does not, see the test plan's
Phase 0.2 for why):

```bash
easy-db-lab kit source add cqlite /path/to/cqlite/easy-db-lab-kits
easy-db-lab kit install cqlite-flight --tag v0.13.1 --flight-port 8815
easy-db-lab kit install trino-cqlite --connector-version 0.13.1 --flight-port 8815 \
  --sidecar-uri "http://$(easy-db-lab ip db0 --private):9043" --trino-image-tag 481
easy-db-lab kit install trino-loadtest --target trino
```

See each kit's own README for lifecycle commands (`start`/`stop`/`status`) and
the test plan for the full run order, prerequisites (Maven Central publish
status, the Trino 481 SPI pin, flight-pod preflight), and failure-triage
process.

## Epic

Parent epic: [#2103 — Distributed load-testing harness for cqlite-flight +
Trino connector on easy-db-lab](https://github.com/pmcfadin/cqlite/issues/2103).
