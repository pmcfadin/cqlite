---
title: Observability
description: Runtime OpenTelemetry traces and metrics — one-command local stack, configuration for every surface, and the metric naming/units catalog.
sidebar:
  label: Observability
  order: 8
---

# Observability

CQLite emits OpenTelemetry **traces** and **metrics** at runtime so you can see
query, read, write, compaction, and Arrow Flight behavior in a real telemetry
backend. This page covers the one-command local stack, configuration for every
surface, and the metric catalog.

This is the *runtime* observability guide. For *offline* CPU/heap profiling
(flamegraphs, dhat), see the profiling docs in the repository
(`docs/profiling.md`).

The runtime foundation must be compiled in: build with the `observability`
feature on the relevant crate. Without it, telemetry calls are inert no-ops.

## One-command local stack

A ready-to-run Docker Compose stack — OpenTelemetry Collector + Jaeger +
Prometheus + Grafana — lives in the repository under
[`docs/observability/`](https://github.com/pmcfadin/cqlite/tree/main/docs/observability).

```bash
docker compose -f docs/observability/docker-compose.yml up
```

| Service | URL | Purpose |
|---------|-----|---------|
| OTLP gRPC | `http://localhost:4317` | CQLite exports here (default) |
| OTLP HTTP | `http://localhost:4318` | with `CQLITE_OTEL_PROTOCOL=http` |
| Jaeger UI | `http://localhost:16686` | traces |
| Prometheus | `http://localhost:9090` | raw metrics |
| Grafana | `http://localhost:3000` | dashboards (anonymous admin) |

The "CQLite — Overview" dashboard (throughput, latency, error rate) auto-loads in
Grafana under the **CQLite** folder.

Then point CQLite at it:

```bash
CQLITE_OTEL_ENABLED=true CQLITE_OTEL_ENDPOINT=http://localhost:4317 \
  cqlite --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5"
```

A `SELECT` produces this span tree in Jaeger:

```
query.execute
└─ query.table_scan            (or query.point_lookup / query.range_scan)
   └─ sstable.partition_lookup.index   (BIG)  or  sstable.partition_lookup.bti_trie  (BTI)
```

Over Arrow Flight the same tree nests under the per-RPC span and honors the
client's `traceparent`:

```
flight.rpc (do_get)
└─ query.execute
   └─ query.table_scan
      └─ sstable.partition_lookup.index
```

## Shared environment variables (`CQLITE_OTEL_*`)

Read by every surface. Booleans accept `1/0`, `true/false`, `yes/no`, `on/off`.

| Variable | Type | Default | Meaning |
|----------|------|---------|---------|
| `CQLITE_OTEL_ENABLED` | bool | `false` | Master switch; when false, init is a no-op. |
| `CQLITE_OTEL_ENDPOINT` | string | `http://localhost:4317` | OTLP collector endpoint (gRPC) or base URL (HTTP). |
| `CQLITE_OTEL_PROTOCOL` | enum | `grpc` | `grpc` or `http` (HTTP/protobuf). |
| `CQLITE_OTEL_SERVICE_NAME` | string | `cqlite` | `service.name` resource attribute. |
| `CQLITE_OTEL_SERVICE_VERSION` | string | crate version | `service.version` resource attribute. |
| `CQLITE_OTEL_SAMPLING_RATIO` | f64 | `1.0` | Trace-ID-ratio sampling probability, clamped to `[0.0, 1.0]`. |
| `CQLITE_OTEL_TIMEOUT_MS` | u64 | `10000` | Exporter export timeout in milliseconds. |

## Per-surface configuration

### CLI

One `--otel-*` flag per setting, each with a `CQLITE_OTEL_*` env fallback
(explicit flag wins; precedence is `config file < env < flag`):
`--otel-enabled`, `--otel-endpoint`, `--otel-protocol`, `--otel-service-name`,
`--otel-service-version`, `--otel-sampling-ratio`, `--otel-timeout-ms`.

```bash
cqlite --otel-enabled true --otel-endpoint http://localhost:4317 \
  --otel-protocol grpc --otel-service-name cqlite-cli \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5"
```

The CLI config file also has an `[observability]` section (keys mirror the env
vars without the prefix): `enabled`, `endpoint`, `protocol`, `service_name`,
`service_version`, `sampling_ratio`, `timeout_ms`.

```toml
[observability]
enabled = true
endpoint = "http://localhost:4317"
protocol = "grpc"
service_name = "cqlite-cli"
sampling_ratio = 1.0
timeout_ms = 10000
```

### Python

`cqlite.open(...)` takes an optional `otel_config` dict (layered over the
environment) and a default `traceparent`; `execute()` / `execute_streaming()`
accept a per-call `traceparent`. Recognised `otel_config` keys: `enabled`,
`endpoint`, `protocol`, `service_name`, `service_version`, `sampling_ratio`,
`timeout_ms` (unknown keys raise `ValueError`).

```python
import cqlite

with cqlite.open(
    "test-data/datasets/sstables",
    schema="test-data/schemas/basic-types.cql",
    otel_config={"enabled": True, "endpoint": "http://localhost:4317",
                 "service_name": "cqlite-python"},
    traceparent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
) as db:
    for row in db.execute("SELECT * FROM test_basic.simple_table LIMIT 5"):
        print(row.to_dict())
```

### Node.js

Configure via the shared `CQLITE_OTEL_*` environment variables before launching
the process (a dedicated option object is tracked under epic #1031):

```bash
CQLITE_OTEL_ENABLED=true CQLITE_OTEL_ENDPOINT=http://localhost:4317 \
  CQLITE_OTEL_SERVICE_NAME=cqlite-node node app.js
```

### Arrow Flight server

Reads the shared `CQLITE_OTEL_*` environment at startup and propagates the
incoming W3C `traceparent` gRPC metadata into CQLite's spans.

```bash
CQLITE_OTEL_ENABLED=true CQLITE_OTEL_ENDPOINT=http://localhost:4317 \
  CQLITE_OTEL_SERVICE_NAME=cqlite-flight cqlite-flight --listen 0.0.0.0:8815
```

## Metric catalog

Names are dot-separated under `cqlite.`; units use UCUM annotations. The OTel
Collector's Prometheus exporter sanitises dotted names to underscores, appends
`_total` to counters, and adds unit suffixes (`_seconds` for `s`, `_bytes` for
`By`) — e.g. `cqlite.read.rows` → `cqlite_read_rows_total`.

| Metric | Instrument | Unit | Bounded attributes |
|--------|-----------|------|--------------------|
| `cqlite.read.rows` | counter | `{row}` | `cqlite.sstable.format` |
| `cqlite.read.bytes` | counter | `By` | `cqlite.sstable.format`, `cqlite.compression` |
| `cqlite.read.partitions` | counter | `{partition}` | `cqlite.sstable.format` |
| `cqlite.read.duration` | histogram | `s` | `cqlite.sstable.format` |
| `cqlite.read.partition_lookup.total` | counter | `1` | `cqlite.result`, `cqlite.query.access_path`, `cqlite.sstable.format` |
| `cqlite.read.bloom.checks` | counter | `1` | `cqlite.result`, `cqlite.sstable.format` |
| `cqlite.storage.open.sstables` | counter | `{sstable}` | (none) |
| `cqlite.storage.open.bytes` | counter | `By` | (none) |
| `cqlite.storage.open.tables` | counter | `1` | (none) |
| `cqlite.sstables.open` | gauge | `{sstable}` | `cqlite.sstable.format` |
| `cqlite.query.duration` | histogram | `s` | `cqlite.subsystem` |
| `cqlite.query.rows` | counter | `{row}` | `cqlite.query.access_path`, `cqlite.query.plan_type` |
| `cqlite.query.rows_scanned` | counter | `{row}` | `cqlite.query.access_path` |
| `cqlite.write.mutations` | counter | `{row}` | (none) |
| `cqlite.write.partitions` | counter | `{partition}` | (none) |
| `cqlite.write.bytes` | counter | `By` | (none) |
| `cqlite.memtable.size_bytes` | gauge | `By` | (none) |
| `cqlite.memtable.rows` | gauge | `{row}` | (none) |
| `cqlite.wal.sync.duration` | histogram | `s` | (none) |
| `cqlite.flush.duration` | histogram | `s` | (none) |
| `cqlite.flush.rows` | counter | `{row}` | (none) |
| `cqlite.flush.bytes` | counter | `By` | (none) |
| `cqlite.flush.sstables` | counter | `{sstable}` | (none) |
| `cqlite.compression.ratio` | histogram | `1` | `cqlite.compression` |
| `cqlite.compaction.duration` | histogram | `s` | (none) |
| `cqlite.compaction.rows_merged` | counter | `{row}` | (none) |
| `cqlite.compaction.bytes_written` | counter | `By` | (none) |
| `cqlite.compaction.sstables_in` | counter | `{sstable}` | (none) |
| `cqlite.compaction.sstables_out` | counter | `{sstable}` | (none) |
| `cqlite.compaction.tombstones_purged` | counter | `{tombstone}` | (none) |
| `cqlite.compaction.lag` | gauge | `{sstable}` | (none) |
| `cqlite.compaction.finalize.duration` | histogram | `s` | (none) |
| `cqlite.compaction.budget.requested` | histogram | `s` | (none) |
| `cqlite.compaction.budget.consumed` | histogram | `s` | (none) |
| `cqlite.errors.total` | counter | `{error}` | `cqlite.error.category`, `cqlite.subsystem` |
| `cqlite.rpc.requests` | counter | `1` | `cqlite.rpc.method`, `cqlite.rpc.status` |
| `cqlite.rpc.duration` | histogram | `s` | `cqlite.rpc.method`, `cqlite.rpc.status` |
| `cqlite.rpc.in_flight` | gauge | `1` | `cqlite.rpc.method` |
| `cqlite.rpc.rows` | counter | `{row}` | `cqlite.rpc.method` |
| `cqlite.rpc.bytes` | counter | `By` | `cqlite.rpc.method` |

Unbounded values (raw error messages, partition keys, full query text) are NEVER
attached as attributes or span fields.
