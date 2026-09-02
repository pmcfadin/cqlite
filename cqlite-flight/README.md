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

### A request must have at least one OUTPUT column (issue #3742)

A ticket whose output column set is empty is refused up front with
`InvalidArgument`, before any Arrow schema message is produced — never with a
mid-stream failure and never with a zero-field schema. Refused shapes:

| Ticket | Why |
|--------|-----|
| `"columns": []` | an explicitly empty projection selects nothing |
| `"columns": ["nope"]` | no projected name exists in the table schema (the error NAMES them) |
| `"aggregation": {"group_by": [], "aggregates": []}` | output columns are `group_by` + `aggregates` |

The rule is **zero total output columns**, not "no aggregates":
`{"group_by": ["c"], "aggregates": []}` — what Trino emits for
`SELECT DISTINCT c` — has one output column and is served, as is a projection
that names an unknown column alongside a real one (it resolves to the real
one). Omit `columns` (or send `null`) to select every column.

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
| `--max-concurrent-scans` (`CQLITE_MAX_CONCURRENT_SCANS`) | **derived: `clamp(2 × P, 2, 64)`** where `P` = hardware threads available to this process (issue #3225) | Admission-control cap on concurrent `do_get` scans (issue #2420). **The default is core-aware as of #3225** — `64` is now the **ceiling**, retained from #2420's blocking-pool / fd sizing, not the value every host gets. Clamped to `[1, Semaphore::MAX_PERMITS]` (an out-of-range value is clamped with an operator warning rather than failing startup). See [sizing](#sizing---max-concurrent-scans-measured-3225). |

`RUST_LOG=info` enables logging (per-SSTable reads, etc.).

### Admission control (`--max-concurrent-scans`)

Each `do_get` acquires an admission permit **after** minimal ticket-syntax validation
but **before** any producer/schema construction or filesystem work; the permit rides
the response stream and releases on completion, client drop, or cancel. When all
permits are held, a request queues briefly and, if none frees in time, is shed with
gRPC **`UNAVAILABLE` before the first batch** — a retryable status that rides the
connector's failover (never `RESOURCE_EXHAUSTED`). Malformed tickets fast-fail
`INVALID_ARGUMENT` without consuming a permit. Queue time is visible in the
`cqlite.rpc.phase` `admission` phase and the `cqlite.flight.admission.*` metrics
(see [observability](../docs/observability/README.md)).

### Sizing `--max-concurrent-scans` (measured, #3225)

**Admitting more concurrent scans than the server has cores costs both throughput and
latency.** This is measured, not folklore: full method, curves and residuals in
[`docs/reports/ws0-3225-report.md`](../docs/reports/ws0-3225-report.md), raw artefacts under
`docs/reports/ws0-3225-artifacts/`.

#### The measured optimum moves with server width

One box (Xeon 8488C, SMT on), one 4M-row single-SSTable corpus, full-scan `do_get`, ramp
`1,2,4,8,16,24,32,64` × 3 reps × 120 s, 126 points. `S` = server physical cores pinned
(both SMT siblings together), `P` = hardware threads = `2 × S` on that host:

| S (physical cores) | P (hw threads) | measured peak N | rows/s at peak | derived default `clamp(2 × P, 2, 64)` |
|---:|---:|---:|---:|---:|
| 1 | 2 | 2 | 240,693 | 4 (−5.0% vs peak — see residuals) |
| 2 | 4 | 8 | 432,360 | 8 ✔ exact |
| 3 | 6 | 12 | 624,848 | 12 ✔ (beats N=16 by +1.73%) |
| 4 | 8 | 16 | 815,748 | 16 ✔ exact |
| 6 | 12 | 24 | 1,173,759 | 24 ✔ exact |

Throughput is **aggregate rows/s**. In byte terms at the widest peak that is 813.5 MB/s
**logical uncompressed** (693.07 B/row) = 230.1 MB/s **on-disk compressed** (196.03 B/row) —
three different bases; the report never collapses them.

#### What over-admission costs, in both currencies

Against each width's own measured peak. `p50` is per-scan latency, and each request here is a
full 4M-row scan, so read the **multiple**, not the absolute:

| S | throughput at N=64 (the pre-#3225 default) | per-scan p50 at N=64 |
|---:|---:|---:|
| 1 | **−21.5%** | **41.95×** the p50 at peak (32.2 s → 22.5 min) |
| 2 | −13.7% | 9.51× |
| 3 | −10.0% | 4.55× |
| 4 | −10.0% | 4.57× |
| 6 | **−7.3%** | 2.94× |

Two things to take from this table. **Latency degrades an order of magnitude faster than
throughput** — a 7% throughput loss at 6 cores is already a 2.9× p50 — and **no width was
optimal at the old constant 64**, not even the widest.

#### The derived default, and how to override it

```
N_default(P) = clamp(2 × P, 2, 64)        P = std::thread::available_parallelism()
```

- `P` is **hardware threads available to this process** — it honours the CPU affinity mask and
  the cgroup v1/v2 CPU quota, so a container limited to 1 CPU on a 96-core node derives from 1,
  not 96. The host's `/proc/cpuinfo` is never read.
- **`64` is the ceiling**, retained verbatim from #2420's blocking-pool (~256) and fd
  (~1024/M) sizing. At `P ≥ 32` the derived default *is* 64, so the change is a strict no-op on
  wide hosts and **no deployment is ever admitted more widely than before #3225**.
- **`2` is the floor** — a single permit serialises every scan, and N=1 was the worst point at
  every measured width.

Provenance is logged at startup on the existing `cqlite-flight starting` event, so a log
capture answers "why is this server admitting 16?":

```
max_concurrent_scans = 16                  # effective, post-clamp
max_concurrent_scans_source = "derived"    # flag | env | derived | derived-fallback
available_parallelism = 8                  # the P actually read; OMITTED when unavailable
```

`derived-fallback` means `available_parallelism()` could not answer; the value is then 64 — the
pre-#3225 behaviour — and is labelled distinctly rather than reported as `derived`.

**Overriding.** Precedence is `--max-concurrent-scans` flag → `CQLITE_MAX_CONCURRENT_SCANS` env
→ derived. An explicit value is never clamped toward the derived one, and it logs `source=flag`
or `source=env`. To restore **exactly** the pre-#3225 behaviour on any host:

```bash
cqlite-flight --data-dir /var/lib/cassandra/data --max-concurrent-scans 64
# or: CQLITE_MAX_CONCURRENT_SCANS=64
```

If you tune by hand, tune toward `2 ×` the hardware threads the server process can actually
use, and verify with the `cqlite.flight.admission.in_use` metric against the `limit` gauge.

#### Two residuals — state them before you rely on the formula

1. **The narrowest width is the formula's one miss: −5.0%.** At `P = 2` the formula gives 4
   while the measured peak on **one SMT core** is 2 (228,657 vs 240,693 rows/s). This is a
   deliberate minimax-regret choice, not an oversight: `P = 2` is reported by two physically
   different machines `available_parallelism` cannot distinguish — one SMT core (measured peak 2)
   and two non-SMT cores, whose closest measured proxy is the 2-physical-core curve above with
   its peak at **N=8**. Choosing 2 would be far off on that machine (its N=2 point, 310,633, is
   well below its 432,360 peak), while 4 is within ~5% of both. If you *know* you are on a single
   SMT core, `--max-concurrent-scans 2` is worth ~5%.
2. **The non-SMT extrapolation is UNVALIDATED.** The formula's basis is hardware threads, so on
   an SMT-on host it admits 4 scans per physical core (the fitted value) but on a **non-SMT**
   host (Graviton, most ARM instances, SMT disabled in firmware) logical == physical and it
   admits **2 per physical core — half the fitted per-core value**. No non-SMT arm has ever been
   measured (#3217 had none; #3225 had no non-SMT host available). On a non-SMT box, measure
   before trusting the default, and `--max-concurrent-scans` is the lever.

Everything above is one box, one corpus, one query shape (`full`), one read path (`bypass`).
Cross-run absolute comparisons on that rig carry ~1.7% run-to-run uncertainty (measured — see
the report's bridge-point section), so treat differences below ~2% as noise.

### Lazy Summary-guided index (operational note, #2412)

Opening a BIG-format SSTable is **O(summary)**: when a `Summary.db` component is
present, `cqlite-flight` reads only the summary at open time and parses index
intervals on demand, so a cold first query no longer pays a full `Index.db` parse.
If `Summary.db` is **absent**, the reader fails closed to **one counted full
Index.db parse** (a `FellBack` full parse). The metrics tell which path ran:
`cqlite.sstable.index_parses_total` counts only full parses (flat on a lazy open),
while `cqlite.sstable.index_interval_parses_total` counts per-lookup interval parses.

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
