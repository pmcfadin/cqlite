# cqlite-flight

An **Arrow Flight** server that exposes a Cassandra node's SSTables as queryable
Arrow streams. It runs **co-located with a Cassandra node**, and on each read it
performs an on-the-fly **compaction merge** of that node's SSTables — leaving the
originals untouched — applies token-range / predicate / projection filters, and
streams the result back as Arrow record batches.

It is the data plane behind the [`trino-connector`](../trino-connector): Trino
discovers nodes via the Cassandra Sidecar, assigns each token range to one
replica, and pulls that range from the replica's `cqlite-flight` endpoint.

## Architecture

```
                      ┌──────────────────────── cqlite-flight (one per C* node) ────────────────────────┐
  Flight client       │                                                                                 │
  (Trino / pyarrow)   │  get_flight_info / get_schema ── parse ticket.ddl ─► TableSchema ─► Arrow schema │
        │  ticket      │                                                                                 │
        ├─────────────►│  do_get(ticket):                                                                │
        │              │     resolve SSTable dir  ── live data dir OR  <table>/snapshots/<name>/         │
        │              │              │                                                                  │
        │              │     KWayMerger (cqlite-core)  ── k-way compaction merge, LWW + tombstones       │
        │              │              │  partition-at-a-time                                             │
        │              │     filter   ── token range (DecoratedKey.token)                                │
        │              │              ── predicates (reuses SELECT's evaluate_predicates)                │
        │              │              ── projection (requested columns only)                             │
        │              │     reconstruct rows (read-path build_row_from_scan → SELECT parity)            │
        │  Arrow       │              │                                                                  │
        ◄──────────────┤     CQL → Arrow (cqlite-core export::arrow_convert) ─► RecordBatch stream       │
                       └─────────────────────────────────────────────────────────────────────────────────┘
```

Key properties:
- **Compaction on read.** Every `do_get` runs a full k-way merge of the table's
  SSTables (resolving last-write-wins and tombstones) — the inputs are never
  modified or removed.
- **SSTable-based, not CQL.** It reads flushed SSTables directly; data only
  appears after a `nodetool flush` (memtable rows are invisible).
- **Snapshot-aware.** Reads a Sidecar snapshot directory when the ticket names
  one, for a consistent file set while Cassandra compacts underneath.
- **Output parity with `SELECT`.** Rows are reconstructed with the same code path
  the query engine uses, so the Arrow output matches a Cassandra `SELECT`.

## gRPC surface (Arrow Flight)

| RPC | Implemented | Purpose |
|-----|-------------|---------|
| `GetFlightInfo` | ✅ | Returns the Arrow schema (from the ticket DDL) + an endpoint/ticket. |
| `GetSchema` | ✅ | Returns just the Arrow schema for a ticket. |
| `DoGet` | ✅ | Streams the merged, filtered table as Arrow record batches. |
| `Handshake`, `ListFlights`, `DoPut`, `DoExchange`, `DoAction`, `ListActions` | ✗ | Unimplemented (read-only server). |

The Arrow schema reflects the **projection** in the ticket, and `uuid`/`timeuuid`
columns carry the Arrow UUID extension metadata.

## Flight ticket contract

Clients address a table with a JSON ticket (the same bytes for `do_get`'s
`Ticket`, or as a `FlightDescriptor` command for `get_flight_info`/`get_schema`).
This is the wire contract with non-Rust clients; see `src/ticket.rs`.

```jsonc
{
  "version": 1,                       // ticket format version
  "keyspace": "ks",
  "table": "tbl",
  "ddl": "CREATE TABLE ks.tbl (...)", // parsed into the TableSchema for the merge
  "snapshot": "cqlite-abc",           // optional; null = live data dir
  "token_start": -3074457345618258602,// optional, exclusive  (range is (start, end])
  "token_end":   3074457345618258602, // optional, inclusive
  "wraparound": false,                // true when start > end (min-token ring segment)
  "columns": ["pk", "v"],             // optional projection; null = all columns
  "predicates": [                     // optional; AND-combined, evaluated per row
    { "column": "v", "op": "Gt", "value": 10 }
  ]
}
```

`op` is one of `Equal`, `In` (value is a JSON array), `Gt`, `Gte`, `Lt`, `Lte`,
`Prefix`. Predicate operands are typed by the column's CQL type. Token-range
filtering uses the token stored on each partition key — no Murmur3 computation.

## Running

```bash
# from source
cargo run -p cqlite-flight -- \
  --data-dir /var/lib/cassandra/data \
  --listen 0.0.0.0:8815 \
  --batch-size 8192

# container — published to GHCR on every release tag (multi-arch amd64/arm64).
# Mount the Cassandra data dir read-only and point --data-dir at it.
docker run --rm -p 8815:8815 \
  -v /var/lib/cassandra:/var/lib/cassandra:ro \
  ghcr.io/pmcfadin/cqlite-flight:latest \
  --data-dir /var/lib/cassandra/data --listen 0.0.0.0:8815
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | (required) | Root holding `<keyspace>/<table>[-<uuid>]/` SSTable dirs. |
| `--listen` | `0.0.0.0:8815` | Flight gRPC listen address. |
| `--batch-size` | `8192` | Max rows per Arrow record batch. |

`RUST_LOG=info` enables logging (per-SSTable reads, etc.).

## Client example (Python / pyarrow)

```python
import json
import pyarrow.flight as flight

client = flight.connect("grpc://localhost:8815")

ticket_json = json.dumps({
    "keyspace": "test_basic",
    "table": "simple_table",
    "ddl": "CREATE TABLE test_basic.simple_table (id uuid PRIMARY KEY, name text)",
    # optional pushdowns:
    # "columns": ["id", "name"],
    # "predicates": [{"column": "name", "op": "Prefix", "value": "a"}],
}).encode()

# Arrow schema only:
info = client.get_flight_info(flight.FlightDescriptor.for_command(ticket_json))
print(info.schema)

# Stream the merged, filtered table:
reader = client.do_get(flight.Ticket(ticket_json))
table = reader.read_all()
print(table.to_pandas())
```

## Building & testing

```bash
cargo build -p cqlite-flight
cargo test  -p cqlite-flight
env RUSTFLAGS="-D warnings" cargo clippy -p cqlite-flight --all-targets
```

Tests build real SSTables in-process via the write engine (no external test data),
covering LWW merge, tombstones, wide-row/clustering, null handling, the UUID Arrow
extension, token/predicate/projection filtering, and a full `do_get` encode/decode
round-trip.

## Limitations (v1)

- Reads **flushed SSTables** only (snapshot or live dir) — not memtables.
- Writes BIG format compatible output; counters are not merged.
- Buffers a table's merged batches in memory per `do_get` (no streaming
  backpressure yet) — matches the merge engine's current memory model.

See `../docs/flight-trino/PLAN.md` for the full design and `JOURNAL.md` for history.
