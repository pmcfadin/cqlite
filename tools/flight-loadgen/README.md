# flight-loadgen

A raw `arrow_flight::FlightServiceClient` (tonic) concurrency-ramp load generator
that drives `FlightService::do_get` **directly** against a running `cqlite-flight`
endpoint — no Trino, no JDBC connector, no `cqlite-core` query engine on the
client path.

Issue #2418, epic #2313 WS1 (the throughput-saturation program).

## Why

Every published `cqlite-flight` throughput floor to date is a *through-Trino*
aggregate (round-10b, #2367), which folds split planning, the JDBC connector,
replica fan-out, and network into the number. `flight-loadgen` establishes the
**server-direct ceiling** underneath that floor: it isolates server-side
thread/fd/memory pressure and the admission-shedding behavior (#2420) by talking
to the server over a plain gRPC channel. It is the client that WS8 (the #2313
concurrency ramp that produces the saturation curve) drives; curve-fitting and
the saturation report are downstream of this tool.

This is a measurement tool, **not** a correctness oracle: row/tombstone/type
parity stays with the sstabledump goldens and the query-semantics oracle. It
measures throughput / latency / shedding, not answer correctness.

## Usage

```bash
# Ramp 1→32 concurrency, 30s per step, mixed workload, against a live server:
flight-loadgen \
  --endpoint http://127.0.0.1:8815 \
  --ticket-template ./keyvalue-template.json \
  --ramp 1,2,4,8,16,32 \
  --step-duration 30s \
  --shape mixed \
  --round round-11-server-direct \
  --out round-11-flight-loadgen.jsonl

# Exercise the full client→server→JSONL pipeline against an in-process fixture
# (no external server needed) — the wiring self-test:
flight-loadgen --self-test
```

### Key flags

| Flag | Meaning | Default |
|------|---------|---------|
| `--endpoint <URL>` | `cqlite-flight` gRPC endpoint (`http://host:port`) | required |
| `--ticket-template <FILE>` | Base ticket JSON (connector-shaped `FlightTicket`: keyspace/table/ddl/snapshot, full ring, no limit) | required |
| `--ramp <LIST>` | Ordered target concurrencies, one step each | `1,2,4,8,16,32` |
| `--step-duration <DUR>` | Per-step hold time (`30s`, `500ms`, `2m`) | `30s` |
| `--shape <SHAPE>` | `point` \| `limit-k` \| `full` \| `mixed` | `mixed` |
| `--limit-k <N>` | `LIMIT` k for the `limit-k`/mixed shape | `100` |
| `--point-width <N>` | Token sub-range width for the `point` shape | `2^40` |
| `--mix <SPEC>` | Mixed weights, e.g. `ptr=0.6,lim=0.3,full=0.1` | `ptr=0.6,lim=0.3,full=0.1` |
| `--seed <N>` | Deterministic RNG seed for ticket selection | `42` |
| `--round <LABEL>` | Round label stamped on each record | `""` |
| `--out <FILE>` | JSONL output file (default: stdout) | stdout |
| `--connect-timeout <DUR>` | Per-worker TCP connect timeout | `5s` |
| `--self-test` | Run the in-process wiring self-test instead | off |

### Workload shapes

Each shape is derived by transforming a **clone** of the operator-supplied base
template — the tool never invents DDL or partition keys:

- `full` — the template as-is (full ring, `limit = None`).
- `limit-k` — the template with `limit = Some(k)`.
- `point` — the template narrowed to a **seeded** token sub-range
  `[t, t + width)`. This is a request-setup/admission-cost *proxy*, not a keyed
  single-partition lookup: it may return zero rows on a sparse sub-range. A true
  keyed read needs a partition-key corpus (a follow-up).
- `mixed` — a seeded weighted draw across the three.

Ticket selection is reproducible from `--seed` (salted by step/worker/iteration),
so two runs with the same seed and ramp request byte-identical tickets;
wall-clock timing only bounds a step's duration, never which data is requested.

## JSONL output — `flight-loadgen.step/v1`

One JSON object per line, one per ramp step:

```json
{
  "schema": "flight-loadgen.step/v1",
  "round": "round-11-server-direct", "endpoint": "http://127.0.0.1:8815",
  "ts_unix_ms": 1784199735680, "seed": 42,
  "step": 3, "target_concurrency": 8, "shape": "mixed",
  "duration_s": 30.0,
  "requests_ok": 0, "requests_unavailable": 0, "requests_error": 0,
  "error_codes": {"Internal": 0},
  "qps": 0.0, "rows_per_s": 0.0, "bytes_per_s": 0.0,
  "rows_total": 0, "bytes_total": 0,
  "latency_ms": {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "samples": 0}
}
```

- `qps == requests_ok / duration_s`; `duration_s` is the step's measured elapsed.
- Latency percentiles are over the step's **`ok` requests only**.
- `requests_unavailable` counts gRPC `UNAVAILABLE` responses — the #2420
  admission shed (retry-safe by the server contract). A transport-layer
  `UNAVAILABLE` is indistinguishable from an admission `UNAVAILABLE` on the wire;
  it is attributed to admission by the server's stated contract.
- `requests_error` counts every other non-success status / transport / decode
  failure, tagged by status code under `error_codes`.
- Responses are **drained and dropped** batch-by-batch (row/byte counters only);
  the full result set is never accumulated, so peak client memory is bounded by
  concurrency × one in-flight batch.

### Mapping to the #2399 round-N metrics template

- `qps` / `latency_ms.p50` / `latency_ms.p99` / per-class counts → the **C
  (throughput)** block, tagged *server-direct*.
- `requests_error` / `error_codes` → the **D (hygiene) `errors_total`** input.
- **Cross-node distribution (a C sub-item) is N/A server-direct** — this tool
  drives a single endpoint, so that field is intentionally absent rather than
  fabricated.

## Self-test

`--self-test` (and the `#[test]` `self_test_produces_a_wellformed_jsonl_record`)
serves a tiny 1-SSTable `keyvalue` fixture over an ephemeral `127.0.0.1:0` port,
runs a concurrency-1, fixed-request-count (non-wall-clock) ramp end-to-end, and
asserts a well-formed JSONL record with `requests_ok >= 1`. It is a normal
workspace test (`cargo test -p flight-loadgen`), **not** an `agent-gate.sh`
component, and never contacts a real cluster.
