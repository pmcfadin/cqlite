# Tasks — flight-loadgen (issue #2418, epic #2313 WS1)

## 1. Crate scaffold (`tools/flight-loadgen`)

- [ ] 1.1 Add `tools/flight-loadgen/Cargo.toml` (`publish = false`, binary
      `flight-loadgen`) — captured by the root `members = ["tools/*"]` glob, no
      root manifest edit; deps from `[workspace.dependencies]` (tonic,
      arrow-flight, arrow, tokio, serde, serde_json, clap) + `hdrhistogram` +
      `rand` (seeded RNG).
- [ ] 1.2 Confirm `cargo build --workspace` and the gate's per-package
      `clippy -D warnings` pick up the crate (workspace membership evidence).

## 2. Client + ticket synthesis

- [ ] 2.1 `--endpoint` connect: build a raw `FlightServiceClient<Channel>` (reuse
      the `benches/flight_do_get.rs` connect pattern; connect-timeout, retry loop).
- [ ] 2.2 Load `--ticket-template <file.json>` into a `FlightTicket`; implement the
      four shape transforms (`full`/`limit-k`/`point`/`mixed`) on clones, keeping
      `keyspace`/`table`/`ddl`/`snapshot` unchanged (spec: shapes requirement).
- [ ] 2.3 Seeded ticket generator salted by (step, worker, iteration); unit test
      that identical seeds reproduce the sequence (spec: determinism scenario).

## 3. Ramp engine

- [ ] 3.1 Parse `--ramp` (ordered concurrencies) + `--step-duration`; per step run
      a worker pool of size `C` that keeps `C` `do_get`s in flight until the step
      deadline.
- [ ] 3.2 Per-worker loop: build ticket → `do_get` → drain-and-drop each
      `RecordBatch` (add rows + `get_array_memory_size`; never collect) → record
      latency + outcome (spec: memory-bound + ramp requirements).

## 4. Metrics + classification + output

- [ ] 4.1 Per-step `hdrhistogram` (reset each step) for p50/p95/p99/max over `ok`
      requests.
- [ ] 4.2 Classify outcomes: `ok` / `unavailable` (gRPC `UNAVAILABLE`, #2420
      admission shed) / `error` (+ code) (spec: classification requirement).
- [ ] 4.3 Emit the `flight-loadgen.step/v1` JSONL record per step to stdout / a
      `--out` file, one object per line, with `qps = requests_ok/duration_s`
      (spec: JSONL requirement + design schema).

## 5. TDD self-test (wiring evidence, not a gate component)

- [ ] 5.1 Reuse the `serve_and_connect` ephemeral-port fixture to serve a tiny
      1-SSTable table in-process; add a `--self-test` subcommand + a `#[test]`.
- [ ] 5.2 Self-test runs a concurrency-1, fixed-request-count (no wall-clock) ramp
      and asserts ≥1 JSONL record with `requests_ok >= 1` and all required fields
      present/parseable (spec: self-test requirement).
- [ ] 5.3 Unit test each shape transform's output ticket fields (spec: shapes
      scenario) and the outcome classifier's `UNAVAILABLE`→`unavailable`,
      other→`error` mapping (spec: classification scenarios).

## 6. Docs + handoff

- [ ] 6.1 A short `tools/flight-loadgen/README.md`: purpose (server-direct ceiling
      under the through-Trino floor), CLI flags, the JSONL schema, and how the
      output maps to the #2399 round-N C-throughput block.
- [ ] 6.2 Note in the README that this is the client WS8 (the #2313 ramp) drives,
      and that cross-node distribution is N/A server-direct (single endpoint).
