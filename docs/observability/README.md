# CQLite Runtime Observability

CQLite emits OpenTelemetry traces and metrics at runtime so you can see query,
read, write, compaction, and Arrow Flight behavior in a real telemetry backend.
This directory makes that usable end-to-end.

> This is the **runtime** observability documentation. For **offline** CPU/heap
> profiling (flamegraphs, dhat, `scripts/profile.sh`), see the
> [Profiling Guide](../profiling.md).

## Contents

- **[Quickstart](quickstart.md)** — `docker compose up`, run a query/flush with
  telemetry on, view the trace in Jaeger and the dashboards in Grafana.
- **[Configuration Reference](configuration.md)** — shared `CQLITE_OTEL_*` env
  vars, per-surface config (CLI, Python, Node.js, Arrow Flight), and the full
  metric naming/units catalog.
- **[`docker-compose.yml`](docker-compose.yml)** — one-command local stack:
  OpenTelemetry Collector + Jaeger + Prometheus + Grafana.
- **`otel-collector/`, `prometheus/`, `grafana/`** — provisioning for the stack
  (collector pipelines, Prometheus scrape config, Grafana datasources +
  auto-loaded dashboards).

## One command

```bash
docker compose -f docs/observability/docker-compose.yml up
```

Then point CQLite at it:

```bash
CQLITE_OTEL_ENABLED=true CQLITE_OTEL_ENDPOINT=http://localhost:4317 \
  cqlite --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5"
```

- Jaeger (traces): http://localhost:16686
- Grafana (dashboards): http://localhost:3000
- Prometheus (raw metrics): http://localhost:9090

See the [Quickstart](quickstart.md) for the full walkthrough.

## Source of truth

The metric names, units, attribute keys, and env vars documented here are
generated to match the implementation exactly:

- Metric catalog + attributes + units: `cqlite-core/src/observability/catalog.rs`
- Env-var config: `cqlite-core/src/observability/config.rs`
- Per-surface config: `cqlite-cli/src/cli_types.rs` / `cqlite-cli/src/config.rs`,
  `bindings/python/src/observability.rs`, `cqlite-flight/src/obs.rs`.
