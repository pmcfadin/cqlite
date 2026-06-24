# Observability Quickstart

End-to-end: bring up a local OpenTelemetry stack, run a CQLite query/flush with
telemetry on, then view the trace in Jaeger and the dashboards in Grafana.

Prerequisites: Docker (with `docker compose`), a CQLite build with the
`observability` feature, and the test datasets fetched
(`bash test-data/scripts/fetch-datasets.sh`).

---

## 1. Start the local OTel stack

One command brings up the OpenTelemetry Collector, Jaeger, Prometheus, and
Grafana:

```bash
docker compose -f docs/observability/docker-compose.yml up
```

Endpoints once it is healthy:

| Service | URL | Purpose |
|---------|-----|---------|
| OTLP gRPC | `http://localhost:4317` | CQLite exports here (default) |
| OTLP HTTP | `http://localhost:4318` | CQLite exports here with `--otel-protocol http` |
| Jaeger UI | http://localhost:16686 | traces |
| Prometheus | http://localhost:9090 | raw metrics |
| Grafana | http://localhost:3000 | dashboards (anonymous admin) |

The "CQLite — Overview" dashboard auto-loads in Grafana under the **CQLite**
folder.

---

## 2. Run CQLite with telemetry on

Build the CLI with the `observability` feature, then run a query pointed at the
local collector. `CQLITE_OTEL_ENDPOINT` defaults to `http://localhost:4317`, so
enabling is the only required flag:

```bash
cargo build --package cqlite-cli --features observability

CQLITE_OTEL_ENABLED=true \
CQLITE_OTEL_ENDPOINT=http://localhost:4317 \
CQLITE_OTEL_SERVICE_NAME=cqlite-cli \
  cargo run --package cqlite-cli --features observability -- \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json
```

To exercise the write path (flush) metrics as well, build with both features and
run a flush:

```bash
cargo build --package cqlite-cli --features "write-support,observability"

CQLITE_OTEL_ENABLED=true \
  cargo run --package cqlite-cli --features "write-support,observability" -- \
  --writable --write-dir /tmp/cqlite-write \
  --schema test-data/schemas/basic-types.cql \
  --flush
```

For the Arrow Flight server, set the same env vars before starting it; for
Python, pass `otel_config={"enabled": True}` to `cqlite.open(...)`. See the
[Configuration Reference](configuration.md) for per-surface details.

---

## 3. View the trace in Jaeger

Open http://localhost:16686, pick the `cqlite-cli` service, and click **Find
Traces**. A `SELECT` produces a span tree like:

```
query.execute                       (root span for the CLI; plan_type attribute)
└─ query.table_scan                 (or query.point_lookup / query.range_scan)
   └─ sstable.partition_lookup.index   (BIG)  or  sstable.partition_lookup.bti_trie  (BTI)
```

When the query arrives over the Arrow Flight server instead of the CLI, the same
tree nests under the per-RPC span, and the client's `traceparent` is honored:

```
flight.rpc (do_get)
└─ query.execute
   └─ query.table_scan
      └─ sstable.partition_lookup.index
```

(SSTable open emits its own `sstable.reader.open*` spans the first time a table
is read.)

---

## 4. View the dashboards in Grafana

Open http://localhost:3000, go to **Dashboards → CQLite → CQLite — Overview**.
Three signal groups populate after a few queries/flushes:

- **Throughput** — read rows/sec (`cqlite.read.rows`), query rows/sec
  (`cqlite.query.rows`), mutations/sec (`cqlite.write.mutations`), flush rows/sec
  (`cqlite.flush.rows`), compaction rows merged/sec
  (`cqlite.compaction.rows_merged`).
- **Latency** — p50/p95/p99 of `cqlite.query.duration`, `cqlite.flush.duration`,
  `cqlite.compaction.duration`, `cqlite.wal.sync.duration`.
- **Error rate** — `cqlite.errors.total` broken out by `cqlite.error.category`
  and `cqlite.subsystem`.

Metrics take a few seconds to appear (Prometheus scrapes the collector every
5s). If a panel is empty, confirm in Prometheus (http://localhost:9090) that a
series such as `cqlite_read_rows_total` exists — that confirms the collector is
receiving CQLite's OTLP metrics.

---

## 5. Tear down

```bash
docker compose -f docs/observability/docker-compose.yml down
```

## Troubleshooting

- **No data anywhere.** Confirm the CLI was built `--features observability` and
  that `CQLITE_OTEL_ENABLED=true`. Without the feature, telemetry is an inert
  no-op.
- **Traces but no metrics (or vice-versa).** Check the collector logs
  (`docker compose -f docs/observability/docker-compose.yml logs otel-collector`);
  the `debug` exporter prints received spans/metrics.
- **Connection refused on 4317.** The collector may still be starting, or
  another process owns the port. `docker compose ... ps` shows container health.
