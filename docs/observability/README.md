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

## Recent instrument families

> **Stopgap.** This hand-maintained list is a temporary bridge — issue #2426 will
> generate the authoritative, operator-facing flight metrics reference directly
> from `cqlite-core/src/observability/catalog.rs` (anti-drift). Until then, the
> catalog is the source of truth; the names below are the recently-added families.

- **`cqlite.flight.admission.*`** — 5 instruments for `do_get` admission control
  (issue #2420): `limit` (gauge, configured `--max-concurrent-scans`), `in_use`
  (gauge, permits held), `waiting` (gauge, requests queued for a permit),
  `rejected_total` (counter, timeout sheds — each a gRPC `UNAVAILABLE`), and
  `wait_seconds` (histogram, time spent queued before admission or rejection).
- **`cqlite.rpc.phase` 5-phase closed set** — the bounded `do_get` phase dimension
  on `cqlite.rpc.phase_duration` is now the closed set
  `validate → admission → resolve → merge_setup → stream` (`admission` and
  `validate` added by #2420), so a stalled request localizes to a phase (queued in
  `admission`, building in `merge_setup`, etc.) from metrics alone.
- **`cqlite.sstable.index_interval_parses_total`** — NEW counter (issue #2412):
  one increment per summary-guided interval parse on a point lookup.
  **`cqlite.sstable.index_parses_total` now counts FULL Index.db parses only** —
  lazy open (#2412) makes a BIG open O(summary), so this counter stays flat on a
  lazy open and increments only when a full parse is actually forced (e.g. an
  absent `Summary.db` → one counted `FellBack` full parse).

## Source of truth

The metric names, units, attribute keys, and env vars documented here are
generated to match the implementation exactly:

- Metric catalog + attributes + units: `cqlite-core/src/observability/catalog.rs`
- Env-var config: `cqlite-core/src/observability/config.rs`
- Per-surface config: `cqlite-cli/src/cli_types.rs` / `cqlite-cli/src/config.rs`,
  `bindings/python/src/observability.rs`, `cqlite-flight/src/obs.rs`.
