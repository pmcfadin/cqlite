# Design — Streaming egress byte budget (issue #2821 / M6)

Revised after issue **#2825 (byte-bounded Arrow egress batch sizing, PR #2906) MERGED**. The
pre-#2825 draft of this document composed a payload figure with a capacity figure and got the
arithmetic wrong twice; §D0 exists so that cannot happen again.

## Context / verified anchors
Line anchors are `main`-relative at authoring time (post-#2825) and will drift; the implementer
re-greps.

| Anchor | What is there |
|---|---|
| `cqlite-flight/src/batch_bytes.rs:126` | `DEFAULT_MAX_BATCH_BYTES = 4 * 1024 * 1024` — per-batch **payload** cap (#2825) |
| `cqlite-flight/src/batch_bytes.rs:144` | `BATCH_BYTES_CAPACITY_FACTOR = 2` — the published payload→capacity conversion, explicitly "published so a consumer — notably issue #2821's per-stream in-flight ceiling — can convert" |
| `cqlite-flight/src/batch_bytes.rs:164` | `BATCH_BYTES_PER_COLUMN_SLACK = 1024`, per Arrow array **node** |
| `cqlite-flight/src/batch_bytes.rs:356` | `worst_case_batch_capacity_bytes(cap, n_array_nodes, widest_row_payload)` |
| `cqlite-flight/src/batch_bytes.rs:66-93` | the module doc block this change must truth-up (the `~56 MiB` count-bounded claim + "TARGET for #2821") |
| `cqlite-flight/src/streaming.rs:59-66` | `DO_GET_CHANNEL_CAPACITY: usize = 4` (batches) + the doc comment that mis-derives the bound and declares the depth "deliberately not a config knob" |
| `cqlite-flight/src/streaming.rs:87` | `IN_FLIGHT_ALLOWANCE: usize = 3` — `#[cfg(test)]`-ONLY, a test-observation bound |
| `cqlite-flight/src/streaming.rs:99-133` | `StreamProbe` — the feature-independent test observation seam to extend |
| `cqlite-flight/src/streaming.rs:149-188` | `ChannelSink::emit`, which races `tx.reserve()` against `cancel.cancelled()` in a biased `select!` inside `Handle::block_on` (runs on a `spawn_blocking` thread) |
| `cqlite-flight/src/streaming.rs:260-282` | `spawn_streaming_from_readers(..., capacity, ...)` — the warm wrapper, must carry the new parameter too |
| `cqlite-flight/src/streaming.rs:290-300` | `spawn_streaming(...)` → `mpsc::channel::<Result<RecordBatch, ProducerError>>(capacity.max(1))` |
| `cqlite-flight/src/streaming.rs:381` | `(encode_do_get(metered, schema_ref, probe), handle)` — **`MeteredDoGetStream` is UPSTREAM of `FlightDataEncoderBuilder`** (the row path; `:440` is the aggregate path) |
| `cqlite-flight/src/streaming.rs:535-573` | `MeteredDoGetStream` fields + `new`; `impl Drop` at `:711` |
| `cqlite-flight/src/streaming.rs:647` | `let batch_bytes = batch.get_array_memory_size() as u64;` in `poll_next` — the drain-side **capacity** measurement, already computed for metrics |
| `cqlite-flight/src/service.rs:307,343,353,459` | `max_batch_bytes` field, default applied in `with_admission`, `with_max_batch_bytes` builder, hand-off to the producer — the plumbing precedent to mirror |
| `cqlite-flight/src/service.rs:885` | the SOLE production spawn site — `spawn_streaming_from_readers(...)`, the warm `DoGetInput::Rows` route |
| `cqlite-flight/src/main.rs:15,64-65,114,129` | clap `Args` is the ONLY config surface; `--max-batch-bytes` uses `#[arg(long, env = …, default_value_t = …)]`, is applied via the builder, and is logged at startup |
| `cqlite-flight/src/wide_row_fixture.rs` | the merged synthetic wide/narrow shapes (`test-util`-gated) — REUSED here, not duplicated |
| `cqlite-flight/src/streaming_tests.rs:115` | `slow_consumer_bounds_produced_batches` — the structural test to model the new one on |
| `cqlite-flight/src/testutil.rs:211` | `build_sstables(&schema, Vec<Vec<Mutation>>)` — takes `wide_row_mutations` directly |

Production residency today: `(4 channel + ~2 in-flight) × 8192 ≈ 49,152` rows × row width. The
`57,344` figure in circulation used the `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE = 3` and over-counts by
~15%.

## D0 — TWO CURRENCIES (read this before touching any number here)

| Currency | Definition | Who uses it | Why |
|---|---|---|---|
| **Payload** | sum of Arrow buffer *lengths* (`cqlite_core::export::arrow_payload_bytes`) | #2825's per-batch cap, `DEFAULT_MAX_BATCH_BYTES = 4 MiB` | estimable *before* the batch exists and monotonic in row count → it can be a trigger |
| **Capacity** | `RecordBatch::get_array_memory_size()` — buffer *capacities* | **this change's per-stream ceiling**, and `streaming.rs:647`'s existing metering | it is the memory actually resident; only readable after the batch exists → it cannot be a trigger, but it is the right unit for a residency ceiling |

`MutableBuffer` doubles from zero, so capacity runs up to **2×** payload (measured 1.001–1.80×
against arrow 53). **`BATCH_BYTES_CAPACITY_FACTOR = 2` is the published conversion and this change
imports it — it does not re-derive a factor.**

> **The failure mode this section exists to prevent:** adding a payload number to a capacity number.
> `4 MiB (payload cap) + 8 MiB (capacity of a max batch) = 12 MiB` is NOT a bound — it mixes units.
> Two rounds of this issue's arithmetic were wrong in exactly that way.

## D1 — Where the governor lives: charge at `emit`, release at the drain

**Chosen: a per-stream capacity-byte credit pool, charged in `ChannelSink::emit` and released when
the batch leaves `MeteredDoGetStream`.** Both endpoints already exist and both speak the same
currency (`get_array_memory_size()` at `streaming.rs:647`), so no new measurement seam is invented.

Mechanism: a shared `EgressCredit` backed by a `tokio::sync::Semaphore` whose permits are the
ceiling expressed in a coarse unit (KiB), so a 6 MiB ceiling is 6144 permits — comfortably inside
`Semaphore::MAX_PERMITS`, and the rounding is always **upward** (conservative). `emit` acquires
`ceil(batch_capacity_bytes / 1KiB)` permits (clamped, see D2) *before* `tx.reserve()`, in the **same
biased `select!`** that already races `cancel.cancelled()`, so a producer parked on credit is woken
by a client disconnect exactly like a producer parked on a full channel is today.

Rejected alternatives:
- **A byte-capacity channel.** `tokio::sync::mpsc` bounds by message count only; there is no
  byte-weighted variant, and swapping in a third-party weighted channel would replace the
  cancellation-aware `reserve()` race (#2264) that is load-bearing for client-disconnect handling.
- **Bounding at batch construction instead.** That is #2825, now MERGED, and it does not bound
  *residency*: N capped batches in a 4-deep channel is still a product, not a ceiling. #2825 bounds
  the residual TERM of this change's contract; it cannot replace the ceiling.
- **Threading `QueryConfig::n` / `enforce_result_budget` into `cqlite-flight`.** Different
  semantics (a cap on a *materialized result set*, terminal on breach) versus a *residency* limiter
  that only ever delays a producer. Reusing the name would mislead operators.

## D2 — The deadlock-avoidance rule and the honest bound (the load-bearing decision)

A single `RecordBatch` may be larger than the entire ceiling — at the merged defaults it routinely
is (a full 4 MiB-payload batch is up to 8 MiB of capacity against a 6 MiB ceiling). A naive "acquire
`n` permits from a pool of `N < n`" blocks forever: the stream wedges and the client hangs.

**Rule: `emit` MUST always admit at least one batch when zero bytes are in flight.** Implemented by
**clamping the request to the pool total**: `permits = min(ceil(capacity/KiB), total_permits)`. When
everything else has drained, an oversized batch acquires the whole pool and proceeds. Progress is
guaranteed for any batch of any size; no deadlock is reachable.

The price is stated openly rather than hidden:

> **Guaranteed contract: peak charged in-flight egress CAPACITY ≤ `ceiling + one maximum batch`.**
> NOT `ceiling`.

**The arithmetic, in capacity currency, at the merged defaults:**

```text
one maximum batch (capacity)
  = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, widest_row_payload)
  = BATCH_BYTES_CAPACITY_FACTOR * max(4 MiB, widest_row_payload)
      + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
  = 2 * 4 MiB + 1 KiB * n_array_nodes                    (schema whose widest row fits the cap)
  = 8 MiB + ~n KiB

contract  = ceiling + one maximum batch
          = 6 MiB (capacity) + 8 MiB (capacity)
          = 14 MiB   <   16 MiB  (ratified B4 per-query working set at concurrency 1)
headroom  = ~2 MiB, less BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
```

Both terms are capacity. The `6 MiB` ceiling is chosen *because* the 4 MiB payload cap converts to
8 MiB of capacity: an 8 MiB ceiling would land at exactly 16 MiB with zero headroom.

**Terms OUTSIDE the governed set** — named here and in the governor's doc comment, because
"`ceiling + one maximum batch`" must not be read as covering them:

1. **The producer's pre-credit batch.** `emit` receives an already-materialized batch and only then
   acquires credit, so while parked, one batch is resident and uncharged. This is the pre-existing
   "+1 send-in-flight" term (it exists today against the channel) and this change neither adds nor
   removes it — but a strict worst case that counts producer-side working memory is
   `contract + one batch`, and the doc must not pretend otherwise.
2. **A single row wider than the 4 MiB cap**, delivered as a one-row batch at its natural width —
   `worst_case_batch_capacity_bytes`'s `max(cap, widest_row_payload)` term. A property of the data.
3. **`BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes`** of fixed per-node allocation (KiB-scale for
   flat schemas; a `map<text,text>` column is four nodes).

**Deferred release — why the residual is ONE batch and not two.** `MeteredDoGetStream` sits UPSTREAM
of `FlightDataEncoderBuilder` (`streaming.rs:381`), and the encoder can pull one batch out ahead of
yielding it (the pre-existing "+1 encoder prefetch" term). If `poll_next` released credit at the
instant it yielded a batch, that prefetched batch would be resident with its credit already returned
— making the true bound `ceiling + 2 × max batch` (18 MiB at the defaults, OUTSIDE B4). Instead
`MeteredDoGetStream` holds the yielded batch's credit in a single `deferred: Option<EgressPermit>`
slot and releases it when the NEXT batch is yielded (assigning the new permit drops the old). At
most one batch is downstream of the credit boundary at any time, so the contract holds as stated.
Cost: one batch's credit is held slightly longer — accepted, because a contract the code actually
satisfies is worth more than a tighter one it does not. **This is the subtlest decision in the
change; do not "simplify" it into release-on-yield.**

## D3 — Credit release must be leak-proof on every termination path

A leaked credit is worse than no credit: the producer wedges on a pool that will never refill, and a
client disconnect mid-stream is the common case that must not do this.

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
`get_array_memory_size()` in `emit`, subtract `get_array_memory_size()` in `poll_next`). Two defects
— an asymmetry hazard if the two measurements ever differ for the same batch (drift ⇒ permanent
credit drift ⇒ eventual wedge), and it leaves the abnormal paths (dropped receiver with batches
still queued, panic unwind) to be hand-audited. RAII removes both classes; the permit carries the
exact amount that was charged.

## D4 — Configuration: mirror the merged `--max-batch-bytes` precedent

```
DEFAULT_MAX_INFLIGHT_EGRESS_BYTES: usize = 6 * 1024 * 1024   // 6 MiB of CAPACITY bytes
ENV_MAX_INFLIGHT_EGRESS_BYTES: &str = "CQLITE_MAX_INFLIGHT_EGRESS_BYTES"
--max-inflight-egress-bytes   #[arg(long, env = …, default_value_t = …)]
```

Plumbing chain (each hop is a wiring-evidence link), mirroring `max_batch_bytes` hop for hop:
`const` → `main.rs` `Args` (`:64`) → `CqliteFlightService::with_egress_budget` (builder mirroring
`with_max_batch_bytes`, `service.rs:353`) → service field → `do_get` spawn site (`service.rs:885`)
→ `spawn_streaming_from_readers` (`streaming.rs:260`) → `spawn_streaming` (`streaming.rs:290`) →
`ChannelSink`. The value is logged at startup alongside `max_batch_bytes` (`main.rs:129`).

**Why 6 MiB — derived, not chosen.** B4 ratifies ≤16Mi as the per-query working set at concurrency
1. The contract is `ceiling + one maximum batch`, and one maximum batch is
`BATCH_BYTES_CAPACITY_FACTOR × DEFAULT_MAX_BATCH_BYTES = 8 MiB` of capacity. `16 − 8 = 8` is the
absolute maximum ceiling; 6 MiB takes 2 MiB of that as headroom for the per-node slack term and for
a batch that runs slightly over the modelled shape. A test asserts the composition from the
constants, so raising either constant fails the build rather than silently voiding B4.

**Default-on, following #2825 rather than admission.** `CqliteFlightService::new` leaves admission
**unconstrained** (#2420, roborev-1699) because admission can *reject* a request with a visible
`UNAVAILABLE`. #2825 then established the opposite posture for a memory bound: the byte-cap is on by
default on every construction path because "an unbounded egress batch is a memory hazard, not a
policy choice" (`service.rs:337-343`). A byte credit likewise can only ever *delay* a producer, so
this change follows #2825: `new()` applies `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`, and an embedder opts
out explicitly via `with_egress_budget(EgressBudget::unbounded())`. This is no longer a departure
from precedent — it is the sibling of the merged one.

## D5 — Composition with the existing governors

Four independent bounds, none removed, whichever binds first wins:

| Governor | Bounds | Currency | Scope |
|---|---|---|---|
| `DO_GET_CHANNEL_CAPACITY = 4` | batch **count** in flight | — | per stream |
| `DEFAULT_MAX_BATCH_BYTES = 4 MiB` (#2825, merged) | ONE batch | payload | per batch |
| **new** in-flight byte credit (6 MiB) | **bytes** in flight | **capacity** | per stream |
| Admission `K = 64` | concurrent admitted scans | — | per server |

At narrow row widths the 4-deep channel still binds first and the byte-cap is a no-op (#2825
measured ~20–300 B/row shapes at 22×–1.7× headroom), so narrow-row behaviour must be proven
unregressed. At wide row widths the per-batch cap bounds each batch and this ceiling bounds how many
may be resident — which is the entire point. Server-wide worst case is
`K × (ceiling + one maximum batch)` = `K × 14 MiB`; this change makes that product finite in bytes
for the first time.

## D6 — Documentation corrections (scoped)

Three source/doc corrections, all of them statements this change makes false:

1. **`streaming.rs:59-66`, `DO_GET_CHANNEL_CAPACITY`**: state production residency as
   ~`(4 + 2) × batch_size` ≈ 49,152 rows and flag it **row-width dependent**; stop citing the
   `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE` as a production quantity; do NOT propagate the stale 57,344
   figure; replace "deliberately not a config knob" with a pointer to `--max-inflight-egress-bytes`
   as the configurable governor of that residency.
2. **`batch_bytes.rs:66-93` + `worst_case_batch_capacity_bytes`'s doc (`:341-342`)**: #2825 wrote,
   correctly for its own tree, that egress is still count-bounded at `~7 × 8 MiB ≈ 56 MiB` and that
   the 14 MiB composition "becomes true only once #2821 lands … a TARGET for the dependent issue".
   This change makes it true, so the text becomes the enforced statement (ceiling name, default,
   currency). The payload-vs-capacity explanation and the published-constant conversion are correct
   and stay.
3. **`docs/flight-trino/JOURNAL.md:659-665`** ("B4 composition for issue #2821"): already states
   `6 + 8 = 14 MiB < 16Mi` in the right currency, but prospectively ("the ceiling **must be**
   budgeted…"). It was deliberately assigned to this issue in the #2906 review. Reword to the
   enforced statement and name the delivery.

Out of scope by design: the dated phase-research snapshots
(`docs/research/phase2-verify-parallelism.md` §2 already carries the 49,152-vs-57,344 correction as
a recorded finding) and `docs/architecture/throughput-program-2026-07.md`, whose M11 line #2825
already owns. Keeping the footprint here preserves 1:1:1:1.

## Test strategy (acceptance evidence)

- **Reuse the merged `cqlite-flight/src/wide_row_fixture.rs`** (issue #2825, `test-util`-gated):
  `wide_row_schema()` (`id int PRIMARY KEY, payload blob, label text`) +
  `wide_row_mutations(n_rows, payload_len)` with the fixed `FIXTURE_TIMESTAMP = 100`, fed straight
  into `crate::testutil::build_sstables(&schema, vec![mutations])`. It is suitable as-is:
  `payload_len` is a parameter, so a batch's capacity is dialled to whatever the ceiling test needs;
  every row has the SAME width, so rows-per-batch is an exact function of the cap; the fill is
  id-derived, so runs are byte-identical; and it never touches the fetched `test_wide_rows` corpus.
  A SECOND wide-row fixture in `test_fixtures.rs` would be duplication — do not add one.
- **Wide-row byte-ceiling test**, modelled on `slow_consumer_bounds_produced_batches`
  (`streaming_tests.rs:115`): a slow consumer reads one batch and pauses; assert the probe's peak
  charged in-flight CAPACITY ≤ `ceiling + max observed batch capacity`. Measured bytes/counts only.
- **Composition test from the published constants**: `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES +
  worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0) ≤ 16 MiB`, so the B4
  claim is enforced by the build and neither constant can drift out from under it.
- **Deferred-release test**: after exactly one batch is yielded, its credit is still charged; it is
  released only when the next batch is yielded. This is the test that stops a future "simplify" from
  turning the bound into `ceiling + 2 × batch`.
- **Narrow-row non-regression test**: at the narrow shape with the default ceiling, the batch-count
  channel still binds and produced-batch behaviour matches the pre-change bound.
- **Drop/cancel credit-release test**: after a mid-stream drop the full pool is available (no wedge).
- **Oversized-batch progress test**: a batch larger than the whole ceiling is still delivered
  (proves the D2 clamp; a naive implementation hangs here).
- **CLI wiring-evidence test**: the flag/env value observably governs a real streamed `do_get`
  through the service, not just a helper in a unit test (`tests/issue_2825_max_batch_bytes_e2e.rs`
  is the shape to follow — it drives the real binary).
- **NO wall-clock threshold assertions** in any correctness path (#2642 / `roborev-lints`). A slow
  consumer is simulated by withholding polls, not by sleeping-and-timing.

## Risks
- **Throughput regression at narrow widths** if 6 MiB binds tighter than the 4-deep channel. Guarded
  by the narrow-row non-regression test; the ceiling is configurable if a deployment needs more.
- **A wedge from a credit-accounting bug** is the worst failure mode (a hung client stream). Guarded
  structurally by RAII release (D3) and behaviourally by the drop/cancel and oversized-batch tests.
- **Currency drift.** Any future reader who adds a payload number to a capacity number reintroduces
  the original error. Guarded by D0, by importing `BATCH_BYTES_CAPACITY_FACTOR` instead of
  re-deriving it, and by the composition test.
- **Channel element type change** (`RecordBatch` → `CreditedBatch`) touches `spawn_streaming`,
  `spawn_streaming_from_readers`, and every test helper that constructs the channel directly.
  Mechanical, but the blast radius is named up front.
