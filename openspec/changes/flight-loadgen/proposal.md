# tools/flight-loadgen — raw FlightServiceClient ramp harness (issue #2418, epic #2313 WS1)

## Why

Epic #2313 (the throughput-saturation program) needs to answer one question the
existing measurements cannot: **where does the `cqlite-flight` server itself
saturate, independent of Trino?** Every published floor to date is a
*through-Trino* aggregate (round-10b, #2367: connector `in.mcfad:cqlite-trino:0.14.2`
+ flight `v0.14.1`, 8 threads/180s, 3-node RF=3 → ~2.34 qps, p50 2.9s, p99 10.0s).
That path folds Trino's split planning, the JDBC connector, replica fan-out, and
network into the number, so it cannot isolate server-side thread/fd/memory
pressure — exactly the Rank 2/3/4 failures the admission work (#2420, shipped) and
the WS2 saturation gauges (#2419) are built around.

There is no tool that drives `FlightService::do_get` **directly**. The only
existing client-over-transport code is `cqlite-flight/benches/flight_do_get.rs`
(one `do_get`, single concurrency, Criterion wall-time — an advisory micro-bench,
not a ramp). WS8 (the concurrency ramp that produces the program's saturation
curve) is blocked until a direct load client exists, and WS1 (#2418) is the
sequencing-first item that unblocks it.

`tools/flight-loadgen` establishes the **server-direct ceiling** underneath the
through-Trino floor: a raw `arrow_flight::FlightServiceClient` (tonic) that offers
a parameterized concurrency ramp of `do_get` requests against a running
`cqlite-flight` endpoint, classifies outcomes (including the #2420 admission-shed
`UNAVAILABLE` distinctly from other errors), and emits JSONL per-step records that
feed the #2399 round-N metrics template and diff cleanly between rounds/builds.

## What Changes

- Add a new binary crate `tools/flight-loadgen` (captured by the existing
  `members = ["tools/*"]` glob — **joins the main workspace**, unlike the excluded
  `fuzz/` crate; justified in design §(a)).
- Provide a `flight-loadgen` binary that:
  - connects a raw `FlightServiceClient<Channel>` to a `--endpoint` (no Trino, no
    `cqlite-core` query engine in the client);
  - drives a **concurrency ramp** — an ordered list of target concurrency levels,
    each held for a fixed duration, maintaining that many in-flight `do_get`s;
  - synthesizes four **workload shapes** — point read, `LIMIT`-k scan, full scan,
    mixed — from a base ticket template, under a **deterministic seed**;
  - **classifies** each request outcome: `ok`, `unavailable` (the admission-shed,
    retry-safe gRPC `UNAVAILABLE`), `error` (any other status/transport failure);
  - **drains and drops** every `RecordBatch` immediately (memory-bounded — never
    accumulates the result set), counting rows and bytes incrementally;
  - emits **JSONL per-step records** (throughput, p50/p95/p99 latency, per-class
    counts, bytes) consumable by the #2399 template.
- Ship a cheap in-process **self-test** (the proven `serve_and_connect`
  ephemeral-port fixture pattern) that exercises the full client→server→JSONL
  pipeline deterministically (request-count-bounded, no wall-clock).

## Non-goals

- **NOT a gate component.** An operator/bench tool; never registered in
  `agent-gate.sh --list`, never run against a real cluster in CI.
- **NOT a correctness oracle.** Row/tombstone/type parity stays with the
  sstabledump goldens and the query-semantics oracle; this tool measures
  throughput/latency/shedding, not answer correctness.
- **NOT a Trino/JDBC path.** Isolating the server is the entire point.
- **NOT a server change.** No edits to `cqlite-flight` server code; it consumes
  the existing `do_get` + `FlightTicket` contract as-is.
- **NOT the ramp analysis / WS8 deliverable itself** — this is the client that
  WS8 drives; curve-fitting and the saturation report are downstream.

## Impact

- New crate `tools/flight-loadgen` (binary + a `--self-test`/`#[test]` harness).
  Deps are already in the workspace (tonic, arrow-flight, arrow, tokio, serde,
  serde_json, clap) plus one bounded-memory histogram dep (design §(d)).
- Unblocks epic #2313 WS8 (the ramp) and feeds #2399 (round-N template) with a
  server-direct C-throughput block.
- Zero runtime-path impact: no `cqlite-flight`/`cqlite-core` source changes.
