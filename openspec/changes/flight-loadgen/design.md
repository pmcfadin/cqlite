# Design — tools/flight-loadgen (issue #2418, epic #2313 WS1)

## Context

The server exposes `FlightService::do_get(Ticket)` where the `Ticket.ticket`
bytes are a JSON [`FlightTicket`] (`cqlite-flight/src/ticket.rs`): `keyspace`,
`table`, `ddl`, optional `snapshot`, an optional `[token_start, token_end]` range
(+ `wraparound`), optional `columns`, `predicates`/`filter`, `aggregation`, and
`limit` (issue #2129 — `LIMIT`-k pushdown; `None` = full range). The client is a
plain `arrow_flight::FlightServiceClient<Channel>` (tonic 0.12) that streams
`FlightData`; `FlightRecordBatchStream` decodes it to Arrow `RecordBatch`es. The
admission layer (#2420, shipped) sheds on permit-wait timeout with gRPC
**`UNAVAILABLE`** *before the first batch* (retry-safe for the connector's #2241
failover) — the load client MUST classify that distinctly from other failures.

`cqlite-flight/benches/flight_do_get.rs` already proves the exact client path we
need — `serve_and_connect` binds `127.0.0.1:0`, serves `CqliteFlightService`
in-process, connects a real `FlightServiceClient`, sends a connector-shaped ticket
JSON, and decodes batches — but at concurrency 1 with Criterion wall-time. This
design reuses that fixture for the self-test and generalizes the client into a
ramp.

The decisions (a)–(g) each state alternatives; §Recommended package selects one
coherent set for Seam-1 approval.

---

## (a) Workspace membership: join the main workspace vs. stand alone like `fuzz/`

- **Join (recommended).** The root `Cargo.toml` already globs
  `members = ["tools/*"]`, so `tools/flight-loadgen` is captured with **no
  manifest change** (opting out would require an explicit `exclude`). Joining
  gives free coverage: `cargo build --workspace` compiles it, the gate's
  per-package `clippy -D warnings` lints it, the file-size ratchet watches it, and
  it shares the single workspace lockfile + `[workspace.dependencies]` pins
  (tonic/arrow-flight/tokio/serde already declared).
- **Stand alone (rejected).** `fuzz/` is excluded for reasons that **do not apply
  here**: it needs a nightly toolchain + libFuzzer sanitizer flags, and being in
  the workspace would force every root `cargo build`/clippy to compile a target
  that only ever runs under `cargo fuzz`. `flight-loadgen` is stable-Rust, needs
  no special toolchain, and *wants* to be compiled/linted by the ordinary build.
- **Cost of joining:** it adds a small binary crate to the default workspace
  compile. Acceptable — it is a thin client over already-compiled deps.

**Decision: JOIN** — no `exclude` entry; rely on the `tools/*` glob.

## (b) Ticket synthesis: base template + shape transforms vs. per-shape ticket files

The client must not invent partition keys or DDL. Operator supplies **one base
ticket template** (`--ticket-template <file.json>`: the connector-shaped
`FlightTicket` — `keyspace`/`table`/`ddl`/`snapshot`, full ring, no limit). The
loadgen derives each shape by transforming a *clone* of the template:

- **full scan** — template as-is (full ring, `limit = None`).
- **limit-k scan** — template + `limit = Some(k)` (`--limit-k`, default e.g. 100).
- **point read** — template + a **seeded narrow token sub-range**
  `[t, t + width)` where `t` is drawn from a seeded RNG over the i64 ring and
  `width` = `--point-width` (a small ring fraction). This exercises the
  per-request **setup/resolve/prune/admission** cost (the #2207/#2295/#2398
  point-read package) deterministically; the same seed reproduces the same tokens
  across rounds for diffing.
- **mixed** — a seeded weighted draw across the three (`--mix ptr=0.6,lim=0.3,full=0.1`).

**Fork (recorded for the owner):** a token-narrowed "point read" is a
*setup-cost proxy*, not a keyed single-partition lookup — it may return zero rows
on a sparse sub-range. Making it a true keyed read needs a partition-key corpus
(a follow-up); v1 measures the request-setup/admission cost under concurrency,
which is what the point-read package targets. Alternative (rejected for v1):
require the operator to supply N explicit per-shape ticket files — more faithful
but far more operator burden and non-reproducible across corpora.

## (c) Ramp model

`--ramp 1,2,4,8,16,32` (ordered target concurrencies) × `--step-duration 30s`.
Per step, a **worker pool of size `C`** keeps `C` `do_get`s in flight: each worker
loops — build next ticket (seeded per (step,worker,iteration)), issue `do_get`,
drain, record outcome — until the step deadline. One JSONL record per step (mixed)
or per (step, shape) when shapes are swept separately (`--shape` selects one, or
`--shape mixed`). Determinism: the RNG is seeded from `--seed` (fixed default) and
salted by (step index, worker index, iteration) so a rerun is byte-reproducible
in ticket selection; wall-clock only bounds the step, never the sampled data.

## (d) Latency percentiles under a memory bound

Need p50/p95/p99/max per step without retaining every sample. Options:
- **`hdrhistogram` crate (recommended)** — fixed-footprint recording histogram,
  O(1) record, accurate high-percentile reads; reset per step so memory is bounded
  regardless of request volume. One new workspace dep, widely used, stable Rust.
- Sorted-`Vec<u64>` of raw samples (rejected) — simplest, but O(requests) memory
  per step: a 32-way pool over a 30s step can be millions of samples → violates the
  memory-bound constraint at exactly the concurrency we most want to measure.

**Decision: `hdrhistogram`**, one histogram per step, reset between steps.

## (e) Outcome classification

Inspect each result: success → `ok`; on `Err(tonic::Status)`, bucket
`Code::Unavailable` → **`unavailable`** (the #2420 admission-shed signal —
retry-safe by server contract), everything else (`Internal`, `InvalidArgument`,
transport errors, decode errors) → **`error`**, tagged with the status code for
the record. Rationale: the ramp's key readout is *the concurrency at which the
server starts shedding* (`unavailable` climbing) vs. *actual failures* (`error`).
Caveat recorded in the record schema: a transport-layer `UNAVAILABLE` is
indistinguishable from an admission `UNAVAILABLE` on the wire; we attribute
`UNAVAILABLE` to admission by the server's stated contract (#2420) and note it.

## (f) Memory-bounded consumption

Each `do_get` response is a `FlightRecordBatchStream`; the worker `poll`s it,
and for every `RecordBatch` adds `batch.num_rows()` and
`batch.get_array_memory_size()` to running counters, then **drops the batch**
immediately. Never collect into a `Vec<RecordBatch>`. Peak client RSS is therefore
O(concurrency × one in-flight batch), independent of result-set size — mirroring
the server-side drain-don't-accumulate rule the program enforces.

## (g) CI smoke: compile-only vs. in-process self-test

- **In-process self-test (recommended).** A `#[test]` (and an equivalent
  `--self-test` subcommand) that reuses the `serve_and_connect` fixture: build a
  tiny 1-SSTable fixture, serve `CqliteFlightService` on `127.0.0.1:0`, run a
  **1-step, concurrency-1, fixed-request-count** ramp (count-bounded, *not*
  duration — no wall-clock), and assert ≥1 well-formed JSONL record with
  `ok >= 1` and every required field present + parseable. Sub-second, single
  transport, ephemeral port (no fixed-port flake). This is **wiring evidence**
  (client → gRPC → server → JSONL), catching JSONL-schema drift and ramp-loop
  regressions that compile-only would miss.
- **Compile-only (rejected).** Cheapest, but lets the JSONL contract and the drain
  loop rot silently — contrary to the "wiring evidence" rule (a green compile is
  not exercise).

The self-test is a **normal workspace test** (`cargo test -p flight-loadgen`,
picked up by the gate's blast-radius mapping when the crate is touched) — it is
**not** registered as a bespoke `agent-gate.sh` component and never contacts a
real cluster.

---

## JSONL record schema (feeds #2399 C-throughput block)

One object per emitted step, e.g.:

```json
{
  "schema": "flight-loadgen.step/v1",
  "round": "<--round label>", "endpoint": "http://host:8815",
  "ts_unix_ms": 0, "seed": 42,
  "step": 3, "target_concurrency": 8, "shape": "mixed",
  "duration_s": 30.0,
  "requests_ok": 0, "requests_unavailable": 0, "requests_error": 0,
  "error_codes": {"Internal": 0},
  "qps": 0.0, "rows_per_s": 0.0, "bytes_per_s": 0.0,
  "rows_total": 0, "bytes_total": 0,
  "latency_ms": {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0, "samples": 0}
}
```

Mapping to #2399: `qps`/`p50`/`p99`/error counts = the **C (throughput)** block
(server-direct); `requests_error`/`error_codes` also feed **D (hygiene)
`errors_total`**. Cross-node distribution (a C sub-item) is **N/A server-direct**
(single endpoint) and is stated as such rather than fabricated. Latency percentiles
are over `ok` requests only (documented in the record).

## Recommended package (for Seam-1 approval)

Join the workspace via the `tools/*` glob (a); base-template + seeded shape
transforms (b); ordered ramp steps × duration with a per-step worker pool and a
salted deterministic seed (c); `hdrhistogram` per step (d); ok/unavailable/error
classification attributing `UNAVAILABLE` to admission by server contract (e);
drain-and-drop batches (f); an in-process count-bounded self-test, not a gate
component (g); the `flight-loadgen.step/v1` JSONL schema above.

## Open questions for the owner

1. **Point-read fidelity (fork in §(b)):** ship v1 as a seeded token-narrowed
   *setup-cost proxy*, or block on a partition-key corpus for true keyed reads?
   (Recommend: ship the proxy now; file keyed-read fidelity as a follow-up.)
2. **`hdrhistogram` as a new workspace dep** — acceptable, or prefer a
   hand-rolled bounded percentile (t-digest/reservoir) to avoid the dep?
3. **Default ramp/step/limit-k/point-width values** — set operator-friendly
   defaults here, or leave all required (no defaults) to force explicit runs?
4. **Should the self-test also live as an example** (`cargo run --example`) for
   docs, in addition to the `#[test]`?
