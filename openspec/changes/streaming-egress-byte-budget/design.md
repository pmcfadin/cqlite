# Design — Streaming egress byte budget (issue #2821 / M6)

## Context / verified anchors
Line anchors are `main`-relative at authoring time and will drift; the implementer re-greps.

| Anchor | What is there |
|---|---|
| `cqlite-flight/src/streaming.rs:59-66` | `DO_GET_CHANNEL_CAPACITY: usize = 4` (batches) + the doc comment that both mis-derives the bound and declares the depth "deliberately not a config knob" |
| `cqlite-flight/src/streaming.rs:85` | `IN_FLIGHT_ALLOWANCE: usize = 3` — `#[cfg(test)]`-ONLY, a test-observation bound |
| `cqlite-flight/src/streaming.rs:140-188` | `ChannelSink` + `ChannelSink::emit`, which races `tx.reserve()` against `cancel.cancelled()` in a biased `select!` inside `Handle::block_on` (runs on a `spawn_blocking` thread) |
| `cqlite-flight/src/streaming.rs:290-300` | `spawn_streaming(..., capacity, ...)` → `mpsc::channel::<Result<RecordBatch, ProducerError>>(capacity.max(1))` |
| `cqlite-flight/src/streaming.rs:535-560` | `MeteredDoGetStream` fields; `impl Drop` at `:711` |
| `cqlite-flight/src/streaming.rs:647` | `let batch_bytes = batch.get_array_memory_size() as u64;` in `MeteredDoGetStream::poll_next` — the drain-side measurement seam, already computed for metrics |
| `cqlite-flight/src/streaming.rs:440` | `encode_do_get(metered, ...)` — **`MeteredDoGetStream` is UPSTREAM of `FlightDataEncoderBuilder`** |
| `cqlite-flight/src/admission.rs:43,51` | `DEFAULT_MAX_CONCURRENT_SCANS = 64`, `ENV_MAX_CONCURRENT_SCANS = "CQLITE_MAX_CONCURRENT_SCANS"` — the plumbing precedent to mirror |
| `cqlite-flight/src/main.rs:25,43-44` | clap `struct Args` is the ONLY config surface (no `Config` struct); `--max-concurrent-scans` uses `#[arg(long, env = …, default_value_t = …)]` |
| `cqlite-flight/src/service.rs:286-302,314,321` | `CqliteFlightService` fields (incl. `batch_size`); `new` → `with_admission` builder precedent |
| `cqlite-flight/src/service.rs:857-863` | the SOLE production spawn site — `spawn_streaming_from_readers(..., DO_GET_CHANNEL_CAPACITY, ...)`, the warm `DoGetInput::Rows` route |
| `cqlite-flight/src/streaming_tests.rs:115` | `slow_consumer_bounds_produced_batches` — the structural test to model the new one on |
| `cqlite-flight/src/test_fixtures.rs:57` | `KEYVALUE_BATCH_SIZE = 8192`; every fixture is the narrow `keyvalue` (`key text PRIMARY KEY, value text`) shape |

Production residency today: `(4 channel + ~2 in-flight) × 8192 ≈ 49,152` rows × unbounded row
width. The `57,344` figure in circulation used the `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE = 3` and
over-counts by ~15%.

## D1 — Where the governor lives: charge at `emit`, release at the drain

**Chosen: a per-stream byte credit pool, charged in `ChannelSink::emit` and released when the batch
leaves `MeteredDoGetStream`.** Both endpoints already exist and already measure the same quantity
(`get_array_memory_size()` at `streaming.rs:647`), so no new measurement seam is invented.

Mechanism: a shared `EgressCredit` backed by a `tokio::sync::Semaphore` whose permits are the
ceiling expressed in a coarse unit (KiB), so an 8 MiB ceiling is 8192 permits — comfortably inside
`Semaphore::MAX_PERMITS`, and the rounding is always **upward** (conservative). `emit` acquires
`ceil(batch_bytes / 1KiB)` permits (clamped, see D2) *before* `tx.reserve()`, in the **same biased
`select!`** that already races `cancel.cancelled()`, so a producer parked on credit is woken by a
client disconnect exactly like a producer parked on a full channel is today.

Rejected alternatives:
- **A byte-capacity channel.** `tokio::sync::mpsc` bounds by message count only; there is no
  byte-weighted variant, and swapping in a third-party weighted channel would replace the
  cancellation-aware `reserve()` race (#2264) that is load-bearing for client-disconnect handling.
- **Bounding at batch construction instead (shrink `batch_size`).** That is #2825's job and it does
  not bound *residency*: N small batches in a 4-deep channel is still an uncapped product. It also
  couples a throughput knob to a memory bound.
- **Threading `QueryConfig::n` / `enforce_result_budget` into `cqlite-flight`.** Different
  semantics (a cap on a *materialized result set*, terminal on breach) versus a *residency* limiter
  that only ever delays a producer. Reusing the name would mislead operators.

## D2 — The deadlock-avoidance rule and the honest bound (the load-bearing decision)

A single `RecordBatch` may be larger than the entire ceiling (one 8192-row batch on a very wide
table can exceed 8 MiB by itself). A naive "acquire `n` permits from a pool of `N < n`" blocks
forever: the stream wedges and the client hangs.

**Rule: `emit` MUST always admit at least one batch when zero bytes are in flight.** Implemented by
**clamping the request to the pool total**: `permits = min(ceil(bytes/KiB), total_permits)`. When
everything else has drained, an oversized batch acquires the whole pool and proceeds. Progress is
therefore guaranteed for any batch of any size, and no deadlock is reachable.

The price of that rule is stated openly rather than hidden:

> **Guaranteed contract: peak in-flight egress payload ≤ `ceiling + one maximum batch`.**
> NOT `ceiling`.

An oversized batch is charged only `ceiling`-worth of credit while resident, so it can overshoot by
up to its own full size. The residual term is exactly one batch, and it is bounded only by how
large a batch can get — which is precisely what **#2825 (T4 byte-bounded batch sizing)** caps. This
change and #2825 together give a true byte bound; alone, this change converts an *unbounded,
row-width-proportional, N-batch* residency into a *bounded ceiling plus one batch*.

**Deferred release — why the residual is ONE batch and not two.** `MeteredDoGetStream` sits
UPSTREAM of `FlightDataEncoderBuilder` (`streaming.rs:440`), and the encoder can pull one batch out
ahead of yielding it (the pre-existing "+1 encoder prefetch" term). If `poll_next` released credit
at the instant it yielded a batch, that prefetched batch would be resident with its credit already
returned — making the true bound `ceiling + 2 × max batch`. Instead, `MeteredDoGetStream` holds the
yielded batch's credit in a single `deferred: Option<EgressPermit>` slot and releases it when the
NEXT batch is yielded (assigning the new permit drops the old one). At most one batch is downstream
of the credit boundary at any time, so the contract above holds as stated. Cost: one batch's worth
of credit is held slightly longer — accepted, because a contract the code actually satisfies is
worth more than a tighter one it does not.

## D3 — Credit release must be leak-proof on every termination path

A leaked credit is worse than no credit: the producer wedges on a pool that will never refill, and
a client disconnect mid-stream is the common case that must not do this.

**Chosen: RAII.** The credit is an owned permit that travels *with* the batch through the channel
(the channel element becomes `Result<CreditedBatch, ProducerError>`, where `CreditedBatch` owns the
`RecordBatch` and its `EgressPermit`). Release is `Drop`, so every path is covered by construction:

| Path | Release |
|---|---|
| Normal drain | `MeteredDoGetStream` yields batch N, drops batch N−1's deferred permit |
| Stream dropped mid-flight (client disconnect) | `MeteredDoGetStream::Drop` drops the deferred permit; dropping `rx` drops every queued `CreditedBatch` and its permit |
| Producer error / panic (`ProducerError`) | the `Err` arm carries no permit; queued `Ok` items still drop normally |
| Cancel fires while `emit` is parked | the `select!` cancel arm returns `Cancelled`; no permit was acquired, or the acquired permit drops with the abandoned batch |

Rejected: **explicit release keyed on a re-measurement at the drain side** (charge
`get_array_memory_size()` in `emit`, subtract `get_array_memory_size()` in `poll_next`). It has two
defects — an asymmetry hazard if the two measurements ever differ for the same batch (drift ⇒
permanent credit drift ⇒ eventual wedge), and it leaves the abnormal paths (dropped receiver with
batches still queued, panic unwind) to be hand-audited. RAII removes both classes; the permit
carries the exact amount that was charged.

## D4 — Configuration: mirror the admission-K precedent exactly

```
DEFAULT_MAX_INFLIGHT_EGRESS_BYTES: usize = 8 * 1024 * 1024      // 8 MiB
ENV_MAX_INFLIGHT_EGRESS_BYTES: &str = "CQLITE_MAX_INFLIGHT_EGRESS_BYTES"
--max-inflight-egress-bytes   #[arg(long, env = …, default_value_t = …)]
```

Plumbing chain (each hop is a wiring-evidence link):
`const` → `main.rs` `Args` → `CqliteFlightService::with_egress_budget` (builder mirroring
`with_admission`) → service field → `service.rs` `do_get` spawn site → `spawn_streaming_from_readers`
→ `spawn_streaming` → `ChannelSink`.

**Why 8 MiB.** B4 ratifies ≤16Mi as the **per-query working set** at concurrency 1. The contract is
`ceiling + one maximum batch`, so `8 MiB + max_batch` must fit under 16 MiB ⇒ headroom for a batch
up to ~8 MiB. At the narrow field shape (8192 rows × ~300 B ≈ 2.4 MB) this is comfortable today;
#2825 makes it an enforced property for wide rows rather than a `batch_size`-dependent hope.
8 MiB is also roughly half of today's ~15 MB narrow-row residency, so the default is a real
tightening, not a no-op.

**Departure from the admission precedent, deliberate:** `CqliteFlightService::new` leaves admission
**unconstrained** (#2420, roborev-1699) because admission can *reject* a request with a visible
`UNAVAILABLE` — silently imposing that on a library embedder would change observable behavior. A
byte credit can only ever *delay* a producer; it cannot turn a working query into an error. So
`new()` applies `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` (bounded by default — this is a memory-safety
posture), and an embedder that wants the old unbounded behavior opts out explicitly via
`with_egress_budget(EgressBudget::unbounded())`. The asymmetry with `with_admission` is intentional
and is recorded here so a reviewer does not read it as an oversight.

## D5 — Composition with the existing governors

Three independent bounds, none removed, whichever binds first wins:

| Governor | Bounds | Scope |
|---|---|---|
| `DO_GET_CHANNEL_CAPACITY = 4` | batch **count** in flight | per stream |
| **new** in-flight byte credit (8 MiB) | **bytes** in flight | per stream |
| Admission `K = 64` | concurrent admitted scans | per server |

At narrow row widths the 4-deep channel still binds first (4 × 2.4 MB ≈ 9.6 MB — the byte ceiling
is reached at about the same point, so narrow-row behaviour is effectively unchanged and must be
proven unregressed). At wide row widths the byte ceiling binds first, which is the entire point.
Server-wide worst case remains `K × (per-stream ceiling + one max batch)`; this change makes that
product finite in bytes for the first time.

## D6 — Doc-comment correction (scoped)

The `DO_GET_CHANNEL_CAPACITY` doc comment (`streaming.rs:59-66`) is revised in place to (a) state
production residency as ~`(4 + 2) × batch_size` ≈ 49,152 rows and flag it as **row-width
dependent**, (b) stop citing the `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE` as a production quantity and
NOT propagate the stale 57,344 figure, and (c) replace "deliberately not a config knob" with a
pointer to the new byte ceiling as the configurable governor.

Scope is deliberately limited to this source comment. `docs/research/phase2-verify-parallelism.md`
§2 already carries the 49,152-vs-57,344 correction as a recorded finding; the other phase-research
docs are dated analysis snapshots and are not rewritten; and
`docs/architecture/throughput-program-2026-07.md:385` is manifest item **M11 / #2825**, corrected by
that issue. Keeping the footprint here preserves 1:1:1:1.

## Test strategy (acceptance evidence)

- **Synthetic wide-row fixture in `cqlite-flight/src/test_fixtures.rs`.** Every fixture today is the
  narrow `keyvalue` shape. The new one is a wide blob / many-column schema built from in-process
  mutations, with deterministic pinned content and a fixed timestamp (like `KEYVALUE_TIMESTAMP`).
  It MUST NOT depend on the fetched `test_wide_rows` dataset — a dataset-dependent test that passes
  vacuously on an absent dataset is exactly the failure mode doctrine forbids.
- **Wide-row byte-ceiling test**, modelled on `slow_consumer_bounds_produced_batches`
  (`streaming_tests.rs:115`): a slow consumer reads one batch and pauses; the test asserts the
  probe's observed peak in-flight BYTES ≤ `ceiling + max observed batch bytes`. Assertions are on
  measured bytes and counts only.
- **Narrow-row non-regression test**: at the `keyvalue` shape with the default ceiling, the
  batch-count channel still binds and produced-batch behaviour matches the pre-change bound.
- **Drop/cancel credit-release test**: after a mid-stream drop, the full pool is available again
  (no wedge). This is the #2264-shaped hazard and gets its own test.
- **Oversized-batch progress test**: a single batch larger than the whole ceiling is still
  delivered (proves the D2 clamp; a naive implementation hangs here).
- **CLI wiring-evidence test**: the flag/env value observably governs a real streamed `do_get`
  through the service, not just a helper constructed in a unit test.
- **NO wall-clock threshold assertions** anywhere in the correctness path (#2642 / `roborev-lints`).
  A slow consumer is simulated by withholding polls, not by sleeping-and-timing.

## Risks
- **Throughput regression at narrow widths** if 8 MiB binds tighter than the 4-deep channel. Guarded
  by the narrow-row non-regression test; the ceiling is configurable if a deployment needs more.
- **A wedge from a credit-accounting bug** is the worst failure mode (a hung client stream). Guarded
  structurally by RAII release (D3) and behaviourally by the drop/cancel and oversized-batch tests.
- **The residual one-batch term** remains row-width-proportional until #2825 lands. Stated in the
  contract rather than papered over, and named as the follow-on.
- **Channel element type change** (`RecordBatch` → `CreditedBatch`) touches the `spawn_streaming`
  plumbing and the test helpers that construct the channel directly
  (`streaming_tests.rs:242,399`). Mechanical, but the blast radius is named up front.
