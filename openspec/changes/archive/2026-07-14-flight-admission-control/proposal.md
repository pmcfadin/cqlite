# Flight do_get admission control — bounded concurrent scans with graceful backpressure (issue #2420, WS4)

## Why

The `cqlite-flight` server has **no admission control**. Nothing bounds how many
concurrent `do_get` scans/merges run at once. Verified on `main`:

- `cqlite-flight/src/main.rs:74` builds the server as
  `Server::builder().add_service(FlightServiceServer::new(service)).serve_with_shutdown(...)`
  — no tonic `concurrency_limit(...)` and no `max_concurrent_streams`.
- `rg 'Semaphore' cqlite-flight/` returns nothing — no application-level ceiling
  gates concurrent work anywhere in the crate.

Under rising concurrency the throughput-saturation research
(`docs/architecture/issue-throughput-saturation-research.md` §C6-WS4) ranks the
consequences #2–#4 in order-of-failure:

- **Blocking-pool queueing (Rank 2).** Each `do_get` consumes up to two
  blocking-pool threads (setup `spawn_blocking` at `service.rs:634`, merge
  `spawn_blocking` at `streaming.rs:317`). Tokio's blocking pool caps at 512, so
  past ~256 concurrent scans, setups queue silently with **no backpressure signal
  to the client** — offered concurrency becomes latency the client cannot see.
- **fd exhaustion (Rank 3).** Each scan opens a fresh fd per SSTable
  (`reader/source.rs`, no reader/fd pool by #815 design) plus a `Summary.db` read
  per SSTable during prune. N scans × M SSTables drive fd count toward the
  container ulimit (~1024) → `EMFILE` → query failures.
- **Unbounded memory (Rank 4).** Peak resident payload is
  O(channel · batch) per scan, so RSS grows ≈ N × per-scan peak with no global
  cap → OOM risk.

In short: with no admission ceiling, offered concurrency translates directly into
unbounded thread/fd/memory pressure and **every in-flight request degrades
together** rather than the server queueing or shedding load gracefully.

Round-10b field floor (#2367, connector 0.14.2, 8 threads/180s, 3-node RF=3):
~2.34 qps, ~48 rows/s, p50 2.9s, p99 10.0s, **0 client errors**, all 3 pods
working; flight-layer `do_get` error-status **2.6%** (down from R10's 14%, largely
cleared by #2397 replica rotation). At 8 threads the server has headroom. WS4's
job is to keep behavior graceful as the WS1 ramp pushes concurrency well past
that, where the research predicts the Rank 2/3/4 failures with no ceiling in place.

- **Milestone:** 0.15. Epic #2313 (flight read-throughput saturation) WS4; epic
  #2403 (cqlite-trino latency/throughput/operations) Lane 2.
- **Design-driven** — the overload behavior (reject vs. queue-and-wait), the
  status returned, and the client contract are **product decisions with no parity
  oracle**; they are exactly the client-contract/semantics calls CLAUDE.md routes
  through OpenSpec + Seam 1. Requires Seam-1 owner approval before implementation.
- **Creates capability** `flight-admission-control`.

## What Changes

- **An application-level admission ceiling on `do_get`.** An owned, cloneable
  `tokio::sync::Semaphore` with `K` permits gates entry to the `do_get` merge.
  A request acquires a permit **before** any SSTable is opened or any batch is
  produced, holds it for the scan's lifetime, and releases it on completion,
  client disconnect, or cancellation. In-flight admitted scans are thereby bounded
  by `K`, capping blocking-pool, fd, and memory pressure at a configured limit
  rather than at an OS ceiling.
- **Graceful overload behavior (queue-and-wait, then shed).** On saturation a
  request waits a bounded time for a permit; if none frees within the timeout it
  is rejected with a status the connector can **fail over** on. Because the reject
  happens before any batch is delivered, failover to another replica is
  correctness-safe (see design.md §b — the client contract is decisive here).
- **A real, wired admission knob.** `--max-concurrent-scans` (CLI flag + env),
  default a conservative fixed value validated by the WS1 ramp; the permit-wait
  timeout is likewise configurable. Per the AH decorative-knob doctrine, setting
  `K` is tested to actually bound in-flight scans to `K`.
- **A coarse tonic transport backstop.** A generous `max_concurrent_streams` on
  the server builder (above `K`) protects the accept loop; the Semaphore, not the
  transport cap, is the real admission ceiling.
- **Admission observability.** New catalog gauges/counters — admission limit,
  permits in use, requests waiting, rejections, permit-wait latency — composing
  with the WS2 (#2419) gauges without duplicating `cqlite.rpc.in_flight`.
- **Permit release is cancel-aware.** The permit is held by an RAII guard tied to
  the existing #2361/#2383 `CancelFlag`/drop-cancel machinery, so a cancelled or
  superseded `do_get` releases its permit promptly with no leak.

## Non-goals

- **No Trino-side scheduling changes.** The connector's split scheduling and
  page-source loop are unchanged; this is transport-level server admission only.
- **No change to #2397 replica rotation.** Rotation distributes primaries;
  admission is what the server adds *on top* of rotation to stay graceful once
  offered concurrency exceeds a single node's capacity.
- **No query-planner / result-byte-budget admission.** This gates transport
  concurrency, not per-query resource accounting (that is the core query engine's
  domain).
- **No change to the existing bounded egress channel** (`DO_GET_CHANNEL_CAPACITY`)
  or the #2316 producer-thread budget — admission composes with them, it does not
  re-count them.
- **No wall-clock in tests.** Concurrency is injected deterministically (held
  permits / barriers); timeouts are injectable, never real-time sleeps.

## Doctrine impact

Adds a `--max-concurrent-scans` operator knob to the flight server; the
`agents-developing/` flight/ops docs and the WS4 research section are updated in
the implementing change. No CLAUDE.md rule change.
