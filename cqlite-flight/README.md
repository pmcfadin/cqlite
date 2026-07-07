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
```

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | (required) | Root holding `<keyspace>/<table>[-<uuid>]/` SSTable dirs. |
| `--listen` | `0.0.0.0:8815` | Flight gRPC listen address. |
| `--batch-size` | `8192` | Max rows per Arrow record batch. |

`RUST_LOG=info` enables logging (per-SSTable reads, etc.).

## Container image (GHCR)

The server publishes as a multi-arch (`linux/amd64` + `linux/arm64`) image to the
GitHub Container Registry — built on every release (`v*`) tag and on demand via
the manual workflow (see [below](#cutting-a-one-off-image-no-release-tag)):

```
ghcr.io/pmcfadin/cqlite-flight
```

**Canonical tag scheme (issue #2117):** every version tag is **v-prefixed**,
matching this repo's own git tag convention (`v0.13.0`, `v0.13.1`, ...) and the
version the connector JAR ships under on the release's git tag:

| Tag | Points at |
|-----|-----------|
| `vX.Y.Z` | An exact release (e.g. `v0.13.1`). |
| `vX.Y` | The latest patch on a minor line (e.g. `v0.13`). |
| `latest` | The most recent **stable** release (prereleases excluded). |
| custom | One-off images cut via the manual workflow (see below). |

The publishing contract: `.github/workflows/flight-image.yml` builds and pushes
this set two ways —

1. **Tag push** (`git push origin vX.Y.Z`): automatic, pushes `vX.Y.Z` + `vX.Y`,
   and moves `latest` (unless the tag has a prerelease suffix, e.g. `v0.13.1-rc1`).
2. **Manual dispatch with `version`** (`gh workflow run flight-image.yml -f
   version=X.Y.Z`, no leading `v`): pushes the identical `vX.Y.Z` + `vX.Y` set a
   tag push would. `latest` is **not** moved unless `move_latest=true` is also
   passed — a dispatch can build from an arbitrary ref, so silently retargeting
   the floating `latest` pointer would be surprising. Use this to backfill a
   release image when the tag-push run didn't produce one (e.g. the connector
   JAR published to Maven Central but the container never ran — issue #2117).

A manual dispatch can *also* be given a free-form `image_tag` (e.g. `dev`,
`0.13.0-rc1`) instead of `version`, for a one-off custom tag unrelated to any
release — see [below](#cutting-a-one-off-image-no-release-tag). `image_tag` is
ignored when `version` is set.

**Historical inconsistency (issue #2117):** before this scheme was normalized,
some releases were manually re-tagged by hand and ended up with a mix of
v-prefixed and bare tags (`v0.12.0`; `v0.13.0` **and** bare `0.13.0`/`0.13`).
Those older bare tags are left in place on GHCR (not deleted) but are **not**
part of the contract — only `vX.Y.Z` / `vX.Y` / `latest` are current and
guaranteed going forward.

Once the GHCR package is public, pulling needs **no authentication** (no
`docker login`):

```bash
docker pull ghcr.io/pmcfadin/cqlite-flight:<tag>   # e.g. latest, or a vX.Y.Z release tag
```

The container exposes the Arrow Flight **gRPC** listener on `:8815` and reads
SSTables from a directory you **mount at runtime** — it ships no data of its own.
Run it co-located with a Cassandra node, mounting that node's data dir
**read-only** and pointing `--data-dir` at it:

```bash
docker run --rm -p 8815:8815 \
  -v /var/lib/cassandra:/var/lib/cassandra:ro \
  -e RUST_LOG=info \
  ghcr.io/pmcfadin/cqlite-flight:latest \
  --data-dir /var/lib/cassandra/data --listen 0.0.0.0:8815
```

Notes:
- **Read-only mount** (`:ro`) is recommended — the server never modifies the
  SSTables, it merges them on the fly into Arrow batches.
- Any directory laid out as `<keyspace>/<table>[-<uuid>]/` works as `--data-dir`;
  it does not have to be a live Cassandra node (e.g. a snapshot or an exported
  SSTable tree mounted from elsewhere).
- The image runs as a non-root user (`uid 10001`); ensure the mounted path is
  readable by that uid.
- The Trino connector is **not** in this image — it ships separately as a Trino
  plugin (see [`../trino-connector`](../trino-connector)).

### Pulling a specific release

```bash
docker pull ghcr.io/pmcfadin/cqlite-flight:vX.Y.Z   # a published release tag
```

Every published image is smoke-tested by the release workflow on both
architectures — it is pulled and run, and the publish fails unless the container
serves the Flight listener on `:8815` — so a tag that lands in GHCR is one that
boots and serves.

### Backfilling a release image

If a `v*` tag was pushed (and the connector JAR published to Maven Central) but
the `cqlite-flight` container was never built — the tag-triggered run didn't
fire or failed silently (issue #2117) — republish it without cutting a new
release, by dispatching the same workflow with the `version` input:

```bash
gh workflow run flight-image.yml --repo pmcfadin/cqlite -f version=0.13.1
```

This builds from the current default-branch ref and pushes the identical
`v0.13.1` + `v0.13` tags a `v0.13.1` tag push would have produced. It does
**not** move `latest` unless you also pass `-f move_latest=true` — pass that
only if this backfilled version is genuinely the most recent stable release.

### Cutting a one-off image (no release tag)

Maintainers can build and push an image on demand without tagging a release:
run the **“cqlite-flight image”** GitHub Actions workflow via *Run workflow*
(`workflow_dispatch`) and supply an `image_tag` (e.g. `dev`), leaving `version`
blank. It publishes `ghcr.io/pmcfadin/cqlite-flight:<image_tag>`. The same
workflow also runs automatically on every `v*` tag.

To build locally instead (requires Docker Buildx and a GHCR login with
`write:packages`):

```bash
# single-arch, load into the local docker daemon
docker build -f cqlite-flight/Dockerfile -t cqlite-flight:dev .

# multi-arch build + push to GHCR (run from the repo root)
docker buildx build -f cqlite-flight/Dockerfile \
  --platform linux/amd64,linux/arm64 \
  -t ghcr.io/pmcfadin/cqlite-flight:dev --push .
```

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
