# Design — Streaming egress byte budget (issue #2821 / M6)

Revised twice. (1) After issue **#2825 (byte-bounded Arrow egress batch sizing, PR #2906) MERGED**:
the pre-#2825 draft composed a payload figure with a capacity figure and got the arithmetic wrong
twice, so §D0 exists to make that impossible. (2) After the charge-at-`emit` placement was shown to
bound at `max(ceiling, max_batch) + max_batch` = 16 MiB — exactly B4, zero headroom, and unfixable
by tuning the ceiling. The owner chose **design A: reserve credit BEFORE materializing** (§D2),
which deletes the additive term instead of budgeting around it.

## Context / verified anchors
Line anchors are `main`-relative at authoring time (post-#2825) and will drift; the implementer
re-greps.

| Anchor | What is there |
|---|---|
| `cqlite-flight/src/batch_bytes.rs:126` | `DEFAULT_MAX_BATCH_BYTES = 4 * 1024 * 1024` — per-batch **payload** cap (#2825) |
| `cqlite-flight/src/batch_bytes.rs:144` | `BATCH_BYTES_CAPACITY_FACTOR = 2` — the published payload→capacity conversion, explicitly "published so a consumer — notably issue #2821's per-stream in-flight ceiling — can convert" |
| `cqlite-flight/src/batch_bytes.rs:164` | `BATCH_BYTES_PER_COLUMN_SLACK = 1024`, per Arrow array **node** (the tree AS FOUND; corrected to `2048` by this change — see D4b) |
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

## D1 — Where the governor lives: RESERVE before materializing, release at the drain

**Chosen (design A): a per-stream capacity-byte credit pool, reserved at the batch boundary BEFORE
`rows_to_record_batch` runs, trued up down to the realized `get_array_memory_size()` immediately
after, and released when the batch leaves `MeteredDoGetStream`.**

The earlier "charge in `ChannelSink::emit`" placement was rejected during this revision because it
cannot reach the target bound: `emit` receives an already-materialized batch, so a parked producer
holds a resident, uncharged batch, and the honest worst case was
`max(ceiling, max_batch) + max_batch` = **16 MiB** at the merged defaults — exactly B4, zero
headroom, and unfixable by tuning the ceiling because the binding term is `2 × max_batch_capacity`.
Reserving first deletes that term outright (see D2).

Mechanism: a shared `EgressCredit` backed by a `tokio::sync::Semaphore` whose permits are the
ceiling expressed in a coarse unit (KiB) — the 8 MiB default is 8192 permits, comfortably inside
`Semaphore::MAX_PERMITS`, with rounding always **upward** (conservative). The producer acquires
`ceil(reservation / 1KiB)` permits (clamped, see D2b) at the batch boundary and then `tx.reserve()`s
the channel slot at emit, both in the **same biased `select!`** that already races
`cancel.cancelled()`, so a producer parked on either resource is woken by a client disconnect.

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
- **Accepting the 16 MiB bound**, or **shrinking #2825's 4 MiB payload cap** to buy headroom. Both
  rejected by the owner: the first spends the entire B4 budget on egress, the second regresses a
  merged, measured throughput parameter to paper over an accounting placement.

## D2 — Reserve before materialize (design A): the mechanics at the merged build sites

The producer already carries everything the reservation needs, so this is not a new estimator — it
is a re-ordering.

At every flush point in both loops, `BatchByteCap::accumulated()` holds the payload estimate for
**exactly** the rows about to be materialized:

| Site | Loop | State at the flush |
|---|---|---|
| `producer.rs:997` byte-cap cut | `drive_merge` | `cut_before` fired, crossing row not yet pushed, `reset()` not yet called ⇒ accumulated = the buffered rows |
| `producer.rs:1005` row-cap cut | `drive_merge` | the pushed row was already `accumulate`d ⇒ accumulated = the buffered rows |
| `producer.rs:1015` tail | `drive_merge` | never reset since the last flush ⇒ accumulated = the buffered rows |
| `producer_stream.rs:214/222/232` | `drive_merge_streaming` | identical three sites, identical rule |

Sequence at each site:

```text
reserve  = worst_case_batch_capacity_bytes(byte_cap.accumulated(), n_array_nodes, 0)
           = BATCH_BYTES_CAPACITY_FACTOR * estimate + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
acquire(min(ceil(reserve/KiB), pool_total))      # parks here; nothing materialized yet
batch    = flush_buffer(&mut buffer)             # rows_to_record_batch — now, under credit
actual   = batch.get_array_memory_size()
release(reserved - actual)                       # TRUE UP DOWNWARD, never upward
emit(CreditedBatch { batch, permit })            # channel slot; may park, batch is charged
```

`BatchSink` grows the reservation step (`CollectSink`'s is a no-op, so the collect/parity path stays
byte-identical and needs no Tokio runtime). The reserve → build → true-up → emit sequence lives in
ONE owning helper called from all six sites, or is expressed as a reservation value that `emit`
consumes — it must not be an unenforced calling convention a future build site can forget.

`n_array_nodes` is counted ONCE per merge by walking the producer's already-built `ArrowSchema`
recursively (a `map<text,text>` column is four nodes). No such helper exists in the tree yet; this
change adds it. **Note the slack term is not optional:** a bare `estimate × BATCH_BYTES_CAPACITY_FACTOR`
under-reserves by `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes`, because the published worst case is
`FACTOR × payload + SLACK × nodes`.

**Why the true-up down is required, not an optimization.** `estimate_arrow_row_bytes` is
deliberately conservative (measured over-shoot 1.18–2× on fixed-width shapes, up to ~3× on
high-fidelity ones) and it is then doubled again by the capacity factor. Holding the full
reservation for the batch's whole channel residency would let a single 4 MiB-payload batch pin the
entire pool, collapsing the stream to lock-step. Releasing `reserved − actual` the instant the batch
exists confines the over-reservation to the materialization window.

**Fail closed if `actual > reserved`.** That is a violated invariant (D2a), not a soft accounting
event: acquiring the difference could block behind the pool and deadlock, and ignoring it would
silently break the very bound this change publishes. The stream terminates with a terminal internal
error naming the invariant, the permit drops normally, and no batch is emitted on a false account.
A `debug_assert` makes it loud in tests; the property test in `arrow_size_tests.rs` is what makes it
unreachable in practice.

## D2a — Cross-issue invariant: this bound rests on #2825's estimator contract

```text
(1)  Σ estimate_arrow_row_bytes(columns, row)  >=  arrow_payload_bytes(batch)
     — cqlite-core/src/export/arrow_size.rs, "Conservatism is a contract, not an aspiration",
       property-tested in arrow_size_tests.rs over fixed-width/text/blob/list/set/map/tuple/UDT/
       JSON/nested-empty/all-null/empty-string shapes.
(2)  get_array_memory_size()  <=  worst_case_batch_capacity_bytes(payload, n_array_nodes, 0)
     — cqlite-flight/src/batch_bytes.rs, from MutableBuffer's power-of-two growth.
```

(1) ∧ (2) ⇒ the pre-materialization reservation is a true upper bound on the realized capacity.
Before this change, a weakening of (1) would have cost #2825 a slightly over-sized batch; after it,
the same weakening silently voids a published memory bound. So the dependency is named at BOTH ends
— a comment at the reservation site pointing at the property test, and a line in `arrow_size.rs`'s
conservatism section naming this ceiling as a dependent consumer.

## D2b — The deadlock-avoidance clamp and the honest bound (the load-bearing decision)

A single `RecordBatch` may be larger than the entire ceiling — at the merged defaults it routinely
is (a full 4 MiB-payload batch is up to `8 MiB + ~2n KiB` of capacity, and an operator may configure a
ceiling far below that). A naive "acquire
`n` permits from a pool of `N < n`" blocks forever: the stream wedges and the client hangs.

**Rule: the governor MUST always admit at least one batch when zero bytes are in flight.**
Implemented by **clamping the reservation to the pool total**:
`permits = min(ceil(reservation/KiB), total_permits)`. When everything else has drained, an
oversized batch acquires the whole pool and proceeds. Progress is guaranteed for any batch of any
size; no deadlock is reachable.

The price is stated openly rather than hidden:

> **Guaranteed contract: peak SERVER-SIDE in-flight egress CAPACITY ≤ `max(ceiling, one maximum batch)`.**

**"SERVER-SIDE" is the operative word, and it is a definition, not a hedge.** The governed set is
the capacity bytes the SERVER holds on one stream's egress path: rows being materialized, batches
queued in the `do_get` channel, and yielded batches the consumer has not yet dropped. It is **NOT**
a bound on total resident bytes including consumer-held batches. Once a consumer takes a batch and
retains it, those bytes are the CONSUMER's memory — the server can neither free nor reuse them — so
the governor stops charging for bytes it no longer controls. See D2c: that framing is exactly what
makes the safety valve correct rather than a compromise, and it is why no consumer behaviour can
hang `do_get`.

**The arithmetic, in capacity currency, at the merged defaults:**

```text
one maximum batch (capacity)
  = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, widest_row_payload)
  = BATCH_BYTES_CAPACITY_FACTOR * max(4 MiB, widest_row_payload)
      + BATCH_BYTES_PER_COLUMN_SLACK * n_array_nodes
  = 2 * 4 MiB + 2 KiB * n_array_nodes                    (schema whose widest row fits the cap)
  = 8 MiB + ~2n KiB

contract  = max(ceiling, one maximum batch)
          = max(12 MiB, 8 MiB + ~2n KiB)                 (12 MiB = the shipped D4a default)
          = 12 MiB           <    16 MiB  (ratified B4 per-query working set at concurrency 1)
headroom  = 4 MiB
```

All terms are capacity. Why `max` and not `+`: under reserve-before-materialize every resident
`RecordBatch` on the egress path holds credit, so their summed *charged* capacity cannot exceed the
pool; the only way realized bytes exceed the pool is the clamp, and a clamped batch holds the ENTIRE
pool, so nothing else is resident beside it. Hence `max`, not a sum. (The pre-revision design's
`+ one maximum batch` was the parked, materialized-but-uncharged batch — deleted by D2, not
re-labelled.)

**Terms OUTSIDE the governed set** — named here and in the governor's doc comment:

1. **The producer's row buffer** (`Vec<QueryRow>`, ≤ `batch_size` rows or one byte-cap's worth of
   payload, plus per-value Rust overhead): resident while rows accumulate, while parked on a
   reservation, and briefly alongside the batch during `rows_to_record_batch` before `buffer.clear()`.
   Not a `RecordBatch`, not visible to `get_array_memory_size()`. PRE-EXISTING and unchanged by this
   change — but real, and the doc must not imply the ceiling covers it.
2. **A single row wider than the 4 MiB cap**, delivered as a one-row batch at its natural width —
   `worst_case_batch_capacity_bytes`'s `max(cap, widest_row_payload)` term. A property of the data.
3. **The aggregate route** (`producer.rs:1055`, `aggregate_paths`): it returns `Vec<RecordBatch>`,
   is handed to `futures::stream::iter` at `streaming.rs:~430-440`, and never touches `ChannelSink`
   — so no reservation applies. Bounded by GROUP count by construction; an explicit non-goal.

Note what is NOT on this list any more: `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes` is now
INSIDE the reservation (D2 computes it via `worst_case_batch_capacity_bytes`), and the parked
pre-credit batch no longer exists.

**Deferred release — why the residual is ONE batch and not two.** `MeteredDoGetStream` sits UPSTREAM
of `FlightDataEncoderBuilder` (`streaming.rs:381`), and the encoder can pull one batch out ahead of
yielding it (the pre-existing "+1 encoder prefetch" term). If `poll_next` released credit at the
instant it yielded a batch, that prefetched batch would be resident with its credit already returned
— reintroducing on the consumer side exactly the uncharged-resident-batch class D2 deleted on the
producer side, and making the true bound `max(ceiling, max batch) + max batch` = 16 MiB. Instead
`MeteredDoGetStream` holds the yielded batch's credit in a single `deferred: Option<EgressPermit>`
slot and releases it at the TOP of the NEXT `poll_next`, before polling the inner stream. **This
release point, not "when the next batch is yielded", is what implementation found to be required —
in both directions.** *Correctness*: `FlightDataEncoder::poll_next` (arrow-flight 53.4.1,
`encode.rs:400-436`) polls its inner stream ONLY when its `FlightData` queue is empty, i.e. after
`encode_batch` consumed and dropped the previous `RecordBatch` — so the top of the next poll is the
first instant the previous batch is provably gone, strictly tighter than release-on-next-yield.
*Liveness*: release-on-next-yield DEADLOCKS whenever the pool is one batch deep (the deferred permit
holds the whole pool → the producer parks reserving the next batch → the consumer waits for that
batch). At the merged defaults a worst-case full batch is exactly the whole pool, so that cycle is
reachable in the DEFAULT configuration; the end-to-end tiny-ceiling test hangs under that variant
(verified by temporarily reverting the release point). At most one batch is downstream of the credit
boundary at any time, so the contract holds as stated. **This is the subtlest decision in the change;
do not "simplify" it into release-on-yield.**

## D2c — The safety valve: nothing a consumer does can wedge the stream (review R1)

Keying the deferred release on the batch DATA's liveness (`Weak::strong_count() == 0` per column)
removed the dependence on the consumer's POLL discipline — but replaced it with a dependence on its
DROP discipline, and that failure mode is strictly worse. A consumer that retains batch N while
awaiting N+1 HANGS: the deferred permit holds the credit, the producer parks in
`EgressCredit::reserve`, and the batch the consumer is waiting for can never be built. At the
shipped defaults a resident batch plus a worst-case reservation already exceed the ceiling, so the
cycle is reachable in the DEFAULT configuration, not only in a corner case. An under-charged metric
is a reporting defect; a hung `do_get` is an outage.

`MeteredDoGetStream::open_safety_valve` closes it. From the `Poll::Pending` arm only, and only when
all three of the following hold, it releases the OLDEST deferred permit:

1. **The channel is empty** — the inner poll returned `Pending`, so no batch is on its way.
2. **A reservation is parked RIGHT NOW** — `EgressObservation::parked_now()`, a GAUGE maintained by
   an RAII `ParkGuard` around the semaphore await. A gauge, not the cumulative park counter: a park
   that has since been satisfied, or one whose future was DROPPED by a cancelled stream, must not
   read as "parked", or the valve would fire on a healthy stream and quietly loosen the bound.
3. **Every charged byte is held by a deferred (consumer-retained) batch** — summing the deferred
   permits' own `charged_bytes` against the pool total. If anything else holds credit (a queued
   batch, an in-flight reservation) the producer's park will clear on its own: that is ordinary
   backpressure, not a wedge.

**Race-freedom is part of the mechanism, not an afterthought.** The stream registers for the
producer's next park (an owned `Notify` future) BEFORE returning `Pending`, so a park landing in the
window between the wedge check and the return still wakes it — otherwise nothing would ever poll a
stream whose consumer is waiting on the batch the wedged producer cannot build. That signal is a
SEPARATE `Notify` from the one the saturation test helper waits on: `notify_one` wakes exactly one
waiter, so a shared signal would have each of them randomly stealing the other's wakeup.

**Why releasing is correct rather than a compromise.** Per D2b's framing, the bound governs
SERVER-SIDE residency. A batch the consumer has taken and is retaining is the consumer's memory:
charging it against the server's pool meters something the server cannot free, cannot reuse and
cannot act on — and doing so is precisely what closes the deadlock cycle. So the valve does not
loosen the bound; it restores the bound's actual subject. The 12 MiB ceiling and the B4 composition
stand unchanged over that quantity.

One permit per firing, oldest first: the minimum that can restore progress. Every firing is counted
(`EgressObservation::safety_valve_releases`), and the real-encoder drains assert that count is ZERO
— so "the valve fires on the normal path" is a test-detectable regression, not a silent loosening.

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
DEFAULT_MAX_INFLIGHT_EGRESS_BYTES: usize = 12 * 1024 * 1024  // 12 MiB of CAPACITY bytes
ENV_MAX_INFLIGHT_EGRESS_BYTES: &str = "CQLITE_MAX_INFLIGHT_EGRESS_BYTES"
--max-inflight-egress-bytes   #[arg(long, env = …, default_value_t = …)]
```

Plumbing chain (each hop is a wiring-evidence link), mirroring `max_batch_bytes` hop for hop:
`const` → `main.rs` `Args` (`:64`) → `CqliteFlightService::with_egress_budget` (builder mirroring
`with_max_batch_bytes`, `service.rs:353`) → service field → `do_get` spawn site (`service.rs:885`)
→ `spawn_streaming_from_readers` (`streaming.rs:260`) → `spawn_streaming` (`streaming.rs:290`) →
`ChannelSink`. The value is logged at startup alongside `max_batch_bytes` (`main.rs:129`).

**Default-on, following #2825 rather than admission.** `CqliteFlightService::new` leaves admission
**unconstrained** (#2420, roborev-1699) because admission can *reject* a request with a visible
`UNAVAILABLE`. #2825 then established the opposite posture for a memory bound: the byte-cap is on by
default on every construction path because "an unbounded egress batch is a memory hazard, not a
policy choice" (`service.rs:337-343`). A byte credit likewise can only ever *delay* a producer, so
this change follows #2825: `new()` applies `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`, and an embedder opts
out explicitly via `with_egress_budget(EgressBudget::unbounded())`. This is no longer a departure
from precedent — it is the sibling of the merged one.

## D4a — Re-evaluating the ceiling under design A: 12 MiB (CORRECTED in review)

6 MiB was approved under the additive model (`ceiling + one max batch ≤ 16Mi` ⇒ ceiling ≤ 8, take 6
for headroom). **That model is gone**, so the value must be re-derived rather than inherited.

### The correction: admission is gated on the RESERVATION, not the realized size

The first revision of this section chose 8 MiB on the reasoning that "every ceiling ≤ one maximum
batch yields the identical `max(...)` worst case, so take the largest such value". Both halves are
true and the conclusion was still wrong, because the deadlock clamp does not compare the ceiling
with the *realized* batch — a reservation is taken BEFORE the batch exists, at the full published
worst case, and it is THAT figure the pool must admit:

```text
one worst-case reservation (3 array nodes, the merged wide-row fixture's shape)
  = worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, 3, 0)
  = 2 × 4 MiB + 2 KiB × 3 = 8,394,752 B  ->  permits_for(..) = 8198
8 MiB pool = 8,388,608 B                 ->  8192 permits
8198 > 8192  =>  EVERY byte-cap-cut batch clamps to the WHOLE pool
```

An 8 MiB default therefore produced exactly the outcome this section rejected 6 MiB to avoid —
strict lock-step on the wide-row path with the 4-deep channel as dead weight — missing by precisely
the `BATCH_BYTES_PER_COLUMN_SLACK × n_array_nodes` term (6 KiB at three nodes).

### 12 MiB

```text
12 MiB = 12,582,912 B = 12288 permits  >=  8198   =>  no clamp
contract = max(12 MiB, 8,394,752 B) = 12 MiB  <=  16 MiB (B4), 4 MiB spare
```

| ceiling | worst case | one worst-case reservation admitted? | narrow-shape batches (192 KiB) |
|---|---|---|---|
| 6 MiB | 8 MiB + slack | **no** — clamps to the whole pool | 32 |
| 8 MiB | 8 MiB + slack | **no** — clamps by 6 permits | 42 |
| **12 MiB** | **12 MiB** | **yes**, 4090 permits spare | 64 |

**What 12 MiB buys, stated honestly** (the standing "state the bound honestly" requirement):

1. **Guaranteed**: a single worst-case reservation is admitted without clamping for every schema of
   at most **2048 Arrow array nodes** — `permits_for(2 × cap + 2 KiB × nodes) = 8192 + 2 × nodes`,
   so the clamp engages from `n_array_nodes ≥ 2049` (a very wide or deeply nested projection; a
   `map<text,text>` column is four nodes). Past that width the documented clamp behaviour takes
   over: the batch acquires the whole pool, is still delivered, and is the only thing resident.
2. **Workload-dependent, NOT guaranteed**: after the true-down to the realized
   `get_array_memory_size()` (measured factor 1.0–1.8 ⇒ 4–7.2 MiB for a full 4 MiB payload batch),
   the residual pool can often admit a second reservation, so the stream typically overlaps two
   batches. At the 1.8× end one resident batch leaves 4.8 MiB — under the 8.2 MiB a second full
   reservation asks for — and the producer parks until the first drains. No claim is made that two
   full-size batches are always in flight; the previous text's "admits ~2 typical batches" was such
   a claim and is withdrawn.
3. **Worst case unchanged in kind**: `max(12 MiB, 8 MiB + slack) = 12 MiB` is 4 MiB below the B4
   ceiling-of-the-ceiling, and the composition test asserts it from the imported constants.
4. Narrow shapes are governed by the 4-deep channel at every candidate value (32–64 batches ≫ 4),
   so none of them regresses the narrow path.

**Guards**: `egress_credit_tests::a_worst_case_default_reservation_does_not_clamp` (FAILS at an
8 MiB default; the clamp threshold at the shipped defaults is `n_array_nodes ≥ 2049`, not the 4097
the 1 KiB slack implied) and `::the_clamp_engages_only_past_the_documented_schema_width` (pins BOTH sides of
the 2048-node boundary, including the documented behaviour once the clamp does engage);
`egress_budget_tests::the_default_ceiling_does_not_clamp_a_real_byte_cap_cut_stream` is the
end-to-end wiring evidence over a genuine multi-batch drain at the shipped defaults.

**Why the ceiling is bounded at all.** B4 ratifies ≤16Mi as the per-query working set at concurrency
1; `max(ceiling, 8 MiB + slack) ≤ 16 MiB` gives a hard ceiling-of-the-ceiling of 16 MiB, and the
composition test asserts it from the imported constants so raising either constant fails the build
rather than silently voiding B4.

## D4b — `BATCH_BYTES_PER_COLUMN_SLACK` corrected to 2 KiB (issue #2932, found in the #2821 review)

The published payload→capacity conversion allowed 1 KiB of fixed allocation per Arrow array node.
That is under the real fixed cost of the commonest node there is: a `Utf8`/`Binary` array built by
`export::arrow_convert` reports **1208 B** at any length from zero up (arrow 53 — the string
builder's 1 KiB default values buffer, plus offsets and struct overhead). So a two-`text`-column
batch of three short rows reports 2416 B against a `2 × payload + 1024 × 2` = 2186 B bound.

Under #2825 alone that was a loose doc claim with no runtime consequence. #2821's
reserve-before-materialize turns the same conversion into an ENFORCED reservation that fails closed,
so the understatement became a terminal `Status::internal` on every narrow-table `do_get` (7
real-transport tests). The fix is the published constant, not a local fudge at the reservation site
— the spec forbids a second definition of the conversion. The defect is tracked as **#2932 (P1)**
against merged #2825 code and fixed HERE because #2821 is blocked on it: a two-file change #2821
immediately depends on does not justify a separate issue → branch → gate → merge → rebase cycle.

`BATCH_BYTES_PER_COLUMN_SLACK = 2048` covers the measured 1208 with 840 B of margin per node.

**Enforcement matches the claim (review R2).** The original guard hand-wrote six shapes (two-text,
empty string, text+blob+int, `list<text>`, `map<text,text>`, all-null) while the constant's doc
claimed enforcement "over the whole `arrow_size_tests` shape corpus" — which was private and unused.
That left `FixedSizeBinary(16)` (uuid/timeuuid), the fixed-width scalars, tuple/UDT (`Struct`),
`set`, deep nesting, `frozen` and the `cql_type = None` flat dispatch arms unverified against a
bound that #2821 turned into a FAIL-CLOSED runtime check. The corpus therefore moves to
`cqlite_core::export::arrow_shape_corpus` behind the opt-in `arrow-shape-corpus` feature (the
`fuzz`/`bench-internals` precedent; `cqlite-flight` enables it as a DEV-dependency only, so no
production build links it), and
`batch_bytes_tests::the_capacity_bound_holds_over_the_shared_shape_corpus` asserts the bound over
every shape at full row count AND truncated to one row — the regime the per-node term exists for.
Measured worst case across that corpus is **1188 B per node** (a one-row `Utf8` batch), so 2048
keeps 860 B of margin; both guards FAIL at 1024. Cost: 1 KiB more reservation per array node — 6 KiB
on a three-node schema against a multi-MiB batch — and the no-clamp schema width above becomes 2048
nodes instead of 4096.

## D5 — Composition with the existing governors

Four independent bounds, none removed, whichever binds first wins:

| Governor | Bounds | Currency | Scope |
|---|---|---|---|
| `DO_GET_CHANNEL_CAPACITY = 4` | batch **count** in flight | — | per stream |
| `DEFAULT_MAX_BATCH_BYTES = 4 MiB` (#2825, merged) | ONE batch | payload | per batch |
| **new** in-flight byte credit (**12 MiB**, D4a as corrected) | **bytes** in flight | **capacity** | per stream |
| Admission `K = 64` | concurrent admitted scans | — | per server |

At narrow row widths the 4-deep channel still binds first and the byte-cap is a no-op (#2825
measured ~20–300 B/row shapes at 22×–1.7× headroom), so narrow-row behaviour must be proven
unregressed. At wide row widths the per-batch cap bounds each batch and this ceiling bounds how many
may be resident — which is the entire point. Server-wide worst case is
`K × max(ceiling, one maximum batch)` = `K × 12 MiB` at the merged batch cap; this change makes that
product finite in bytes for the first time.

## D6 — Documentation corrections (scoped)

Three source/doc corrections, all of them statements this change makes false:

1. **`streaming.rs:59-66`, `DO_GET_CHANNEL_CAPACITY`**: state production residency as
   ~`(4 + 2) × batch_size` ≈ 49,152 rows and flag it **row-width dependent**; stop citing the
   `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE` as a production quantity; do NOT propagate the stale 57,344
   figure; replace "deliberately not a config knob" with a pointer to `--max-inflight-egress-bytes`
   as the configurable governor of that residency.
2. **`batch_bytes.rs:66-93` + `worst_case_batch_capacity_bytes`'s doc (`:341-342`)**: #2825 wrote,
   correctly for its own tree, that egress is still count-bounded at `~7 × 8 MiB ≈ 56 MiB` and that
   the composition "becomes true only once #2821 lands … a TARGET for the dependent issue".
   This change makes it true, so the text becomes the enforced statement (ceiling name, default,
   currency, and `max(ceiling, one maximum batch)` in place of the `6 + 8 = 14 MiB` additive sketch,
   which reserve-before-materialize supersedes). The payload-vs-capacity explanation and the published-constant conversion are correct
   and stay.
3. **`docs/flight-trino/JOURNAL.md:659-665`** ("B4 composition for issue #2821"): already states
   `6 + 8 = 14 MiB < 16Mi` in the right CURRENCY, but with the additive model and prospectively
   ("the ceiling **must be** budgeted…"). It was deliberately assigned to this issue in the #2906
   review. Reword to the enforced `max(ceiling, one maximum batch)` statement, record that
   reserve-before-materialize is what removed the additive term, and name the delivery.

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
- **Reserve-before-materialize test**: with the pool exhausted, the producer parks at the batch
  boundary having materialized NOTHING — observable as "batches built" not advancing while a
  reservation is pending, so a regression back to charge-at-emit fails here.
- **Both-loops test**: the ceiling assertion is driven through `drive_merge` AND
  `drive_merge_streaming`; a governor wired into one loop only fails the other.
- **True-up-down test**: on an over-estimated shape, charged credit after materialization tracks the
  REALIZED `get_array_memory_size()`, not the reservation (guards pool starvation / lock-step).
- **Fail-closed test**: a simulated `actual > reserved` terminates the stream with the invariant
  error, leaks no credit, and never emits on a false account.
- **Wide-row byte-ceiling test**, modelled on `slow_consumer_bounds_produced_batches`
  (`streaming_tests.rs:115`): a slow consumer reads one batch and pauses; assert the probe's peak
  charged in-flight CAPACITY ≤ `ceiling + max observed batch capacity`. Measured bytes/counts only.
- **Composition test from the published constants**: `max(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES,
  worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0)) ≤ 16 MiB`, so the B4
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
- **Throughput regression** if the ceiling binds tighter than the 4-deep channel. Narrow shapes are
  unaffected (32–42 batches' worth of credit versus a 4-deep channel) and guarded by the
  non-regression test; the WIDE path at a 6 MiB ceiling admits only ONE full-size batch at a time
  (D4a) — the reason 8 MiB is recommended. The ceiling is configurable either way.
- **Over-reservation starving the pool** if the true-up-down step is dropped or misplaced: the
  estimator over-shoots 1.18–3×, so holding the reservation would serialize the stream. Guarded by
  the true-up test asserting charged credit tracks the REALIZED capacity, not the reservation.
- **Estimator-contract drift (D2a)**: a future weakening of `Σ estimate >= payload` silently
  under-reserves here. Guarded by the named cross-issue invariant at both ends, the property test
  it points at, and the fail-closed `actual > reserved` path.
- **A wedge from a credit-accounting bug** is the worst failure mode (a hung client stream). Guarded
  structurally by RAII release (D3) and behaviourally by the drop/cancel and oversized-batch tests.
- **Currency drift.** Any future reader who adds a payload number to a capacity number reintroduces
  the original error. Guarded by D0, by importing `BATCH_BYTES_CAPACITY_FACTOR` instead of
  re-deriving it, and by the composition test.
- **Channel element type change** (`RecordBatch` → `CreditedBatch`) touches `spawn_streaming`,
  `spawn_streaming_from_readers`, and every test helper that constructs the channel directly.
  Mechanical, but the blast radius is named up front.
