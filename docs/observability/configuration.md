# Observability Configuration Reference

CQLite emits OpenTelemetry traces and metrics at runtime. This page is the
configuration reference for every surface (shared env vars, CLI, Python, Node.js,
Arrow Flight) and the canonical metric naming/units catalog.

> This complements — it does not replace — the offline
> [Profiling Guide](../profiling.md), which covers flamegraphs and heap
> profiling with `scripts/profile.sh`.

The runtime observability foundation must be compiled in: build with the
`observability` feature on the relevant crate to enable OTLP export. Without it,
all telemetry calls are inert no-ops, and configuration still parses but exports
nothing.

---

## Shared environment variables (`CQLITE_OTEL_*`)

These are read by every surface through
`cqlite_core::observability::ObservabilityConfig::from_env`. Booleans accept
`1/0`, `true/false`, `yes/no`, `on/off` (case-insensitive). Source of truth:
`cqlite-core/src/observability/config.rs`.

| Variable | Type | Default | Meaning |
|----------|------|---------|---------|
| `CQLITE_OTEL_ENABLED` | bool | `false` | Master switch; when false, `init` is a no-op even with the feature on. |
| `CQLITE_OTEL_ENDPOINT` | string | `http://localhost:4317` | OTLP collector endpoint (gRPC) or base URL (HTTP). |
| `CQLITE_OTEL_PROTOCOL` | enum | `grpc` | `grpc` or `http` (HTTP/protobuf). |
| `CQLITE_OTEL_SERVICE_NAME` | string | `cqlite` | `service.name` resource attribute. |
| `CQLITE_OTEL_SERVICE_VERSION` | string | crate version | `service.version` resource attribute. |
| `CQLITE_OTEL_SAMPLING_RATIO` | f64 | `1.0` | Trace-ID-ratio sampling probability, clamped to `[0.0, 1.0]`. |
| `CQLITE_OTEL_TIMEOUT_MS` | u64 | `10000` | Exporter export timeout in milliseconds. |

Unparseable values fall back to the documented default rather than erroring, so
a typo never crashes the host process.

Copy-paste example (point any surface at the local stack):

```bash
export CQLITE_OTEL_ENABLED=true
export CQLITE_OTEL_ENDPOINT=http://localhost:4317
export CQLITE_OTEL_PROTOCOL=grpc
export CQLITE_OTEL_SERVICE_NAME=cqlite
export CQLITE_OTEL_SAMPLING_RATIO=1.0
export CQLITE_OTEL_TIMEOUT_MS=10000
```

---

## Per-surface configuration

Every surface ultimately builds the same
`cqlite_core::observability::ObservabilityConfig`. Each starts from the
`CQLITE_OTEL_*` environment as a baseline, then applies surface-specific
overrides.

### CLI

The `cqlite` CLI exposes one `--otel-*` flag per setting. Each flag carries its
`CQLITE_OTEL_*` env fallback (the explicit flag wins). Precedence is
`config file < env < flag`. Source: `cqlite-cli/src/cli_types.rs`.

| Flag | Value | Env fallback |
|------|-------|--------------|
| `--otel-enabled` | `BOOL` | `CQLITE_OTEL_ENABLED` |
| `--otel-endpoint` | `URL` | `CQLITE_OTEL_ENDPOINT` |
| `--otel-protocol` | `PROTO` (`grpc`/`http`) | `CQLITE_OTEL_PROTOCOL` |
| `--otel-service-name` | `NAME` | `CQLITE_OTEL_SERVICE_NAME` |
| `--otel-service-version` | `VER` | `CQLITE_OTEL_SERVICE_VERSION` |
| `--otel-sampling-ratio` | `RATIO` | `CQLITE_OTEL_SAMPLING_RATIO` |
| `--otel-timeout-ms` | `MS` | `CQLITE_OTEL_TIMEOUT_MS` |

```bash
cqlite \
  --otel-enabled true \
  --otel-endpoint http://localhost:4317 \
  --otel-protocol grpc \
  --otel-service-name cqlite-cli \
  --schema test-data/schemas/basic-types.cql \
  --data-dir test-data/datasets/sstables \
  --query "SELECT * FROM test_basic.simple_table LIMIT 5" \
  --out json
```

The CLI config file also has an `[observability]` section
(`cqlite-cli/src/config.rs`). Keys mirror the env vars (without the
`CQLITE_OTEL_` prefix); all keys are optional and fall through to core defaults:

```toml
[observability]
enabled = true
endpoint = "http://localhost:4317"
protocol = "grpc"
service_name = "cqlite-cli"
service_version = "0.12.0"
sampling_ratio = 1.0
timeout_ms = 10000
```

### Python

`cqlite.open(...)` takes an optional `otel_config` dict (layered over the
`CQLITE_OTEL_*` environment) and an optional W3C `traceparent` string used as the
default parent for every per-call span. `execute()` /
`execute_streaming()` also accept a per-call `traceparent` that overrides the
open-time default. Source: `bindings/python/src/observability.rs`,
`bindings/python/src/database.rs`.

Recognised `otel_config` keys (unknown keys raise `ValueError`):
`enabled`, `endpoint`, `protocol` (`grpc`/`http`), `service_name`,
`service_version`, `sampling_ratio`, `timeout_ms`.

```python
import cqlite

with cqlite.open(
    "test-data/datasets/sstables",
    schema="test-data/schemas/basic-types.cql",
    otel_config={
        "enabled": True,
        "endpoint": "http://localhost:4317",
        "protocol": "grpc",
        "service_name": "cqlite-python",
        "sampling_ratio": 1.0,
        "timeout_ms": 10000,
    },
    # Optional: re-parent CQLite spans under your existing trace.
    traceparent="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
) as db:
    for row in db.execute("SELECT * FROM test_basic.simple_table LIMIT 5"):
        print(row.to_dict())
```

### Node.js

The Node.js bindings do not yet expose a dedicated OTel option object; configure
them through the shared `CQLITE_OTEL_*` environment variables before launching
the process. (Tracked under epic #1031.)

```bash
CQLITE_OTEL_ENABLED=true \
CQLITE_OTEL_ENDPOINT=http://localhost:4317 \
CQLITE_OTEL_SERVICE_NAME=cqlite-node \
node app.js
```

### Arrow Flight server

`cqlite-flight` reads the shared `CQLITE_OTEL_*` environment at startup via
`ObservabilityConfig::from_env()` and installs the exporter for the process
lifetime. It also propagates the incoming W3C `traceparent` gRPC metadata so a
client's distributed trace continues into CQLite. Source:
`cqlite-flight/src/main.rs`, `cqlite-flight/src/obs.rs`.

```bash
CQLITE_OTEL_ENABLED=true \
CQLITE_OTEL_ENDPOINT=http://localhost:4317 \
CQLITE_OTEL_PROTOCOL=grpc \
CQLITE_OTEL_SERVICE_NAME=cqlite-flight \
cqlite-flight --listen 0.0.0.0:8815
```

---

## Metric naming / units catalog

Generated to match `cqlite-core/src/observability/catalog.rs` exactly. Names are
dot-separated under the `cqlite.` root; units use UCUM annotations
(`{row}`, `By`, `s`, `1`, `{partition}`, `{sstable}`, `{tombstone}`, `{error}`).

> **Prometheus name mangling.** The OTel Collector's Prometheus exporter
> sanitises dotted names to underscores, appends `_total` to counters, and adds
> unit suffixes (`_seconds` for `s`, `_bytes` for `By`). For example,
> `cqlite.read.rows` → `cqlite_read_rows_total`,
> `cqlite.query.duration` → `cqlite_query_duration_seconds_bucket/_sum/_count`.
> The provided Grafana dashboard uses the sanitised forms.

### Read path

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

### Query engine

| Metric | Instrument | Unit | Bounded attributes |
|--------|-----------|------|--------------------|
| `cqlite.query.duration` | histogram | `s` | `cqlite.subsystem` |
| `cqlite.query.rows` | counter | `{row}` | `cqlite.query.access_path`, `cqlite.query.plan_type` |
| `cqlite.query.rows_scanned` | counter | `{row}` | `cqlite.query.access_path` |

### Write path

| Metric | Instrument | Unit | Bounded attributes |
|--------|-----------|------|--------------------|
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

### Compaction & maintenance

| Metric | Instrument | Unit | Bounded attributes |
|--------|-----------|------|--------------------|
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

### Errors

| Metric | Instrument | Unit | Bounded attributes |
|--------|-----------|------|--------------------|
| `cqlite.errors.total` | counter | `{error}` | `cqlite.error.category`, `cqlite.subsystem` |

### Arrow Flight gRPC service

| Metric | Instrument | Unit | Bounded attributes |
|--------|-----------|------|--------------------|
| `cqlite.rpc.requests` | counter | `1` | `cqlite.rpc.method`, `cqlite.rpc.status` |
| `cqlite.rpc.duration` | histogram | `s` | `cqlite.rpc.method`, `cqlite.rpc.status` |
| `cqlite.rpc.in_flight` | gauge | `1` | `cqlite.rpc.method` |
| `cqlite.rpc.rows` | counter | `{row}` | `cqlite.rpc.method` |
| `cqlite.rpc.bytes` | counter | `By` | `cqlite.rpc.method` |

### Bounded attribute keys

These are the only attribute keys CQLite attaches to catalog metrics; each has a
closed value space so cardinality stays bounded. Source:
`cqlite-core/src/observability/catalog.rs` (`mod attr`).

| Attribute key | Value space |
|---------------|-------------|
| `cqlite.error.category` | low-cardinality error category (≈10 values) |
| `cqlite.subsystem` | e.g. `reader`, `query`, `compaction`, `python`, `flight` |
| `cqlite.sstable.format` | `big`, `bti` |
| `cqlite.compression` | `lz4`, `snappy`, `none`, … |
| `cqlite.result` | `hit`, `miss` |
| `cqlite.read.lookup_route` | `index`, `bti_trie` |
| `cqlite.query.access_path` | `full_scan`, `partition_lookup`, `multi_partition_lookup`, `clustering_slice`, `fallback_full_scan` |
| `cqlite.query.plan_type` | `table_scan`, `point_lookup`, `index_scan`, `range_scan`, `aggregation` |
| `cqlite.rpc.method` | fixed `FlightService` method set (`do_get`, `get_flight_info`, `get_schema`, `handshake`, …) |
| `cqlite.rpc.status` | `ok`, `error` |

> Unbounded values (raw error messages, partition keys, full query text) are
> NEVER attached as attributes or span fields.
