# Tasks — Streaming egress byte budget (issue #2821 / M6)

Branch `issue-2821-streaming-result-budget`. Line anchors are `main`-relative post-#2825 and will
drift — re-grep before editing. Every stage names the surface it exercises. Tests are written FIRST
and must fail on pre-change `main`.

**Currency rule (design D0): the ceiling is CAPACITY bytes (`get_array_memory_size()`); #2825's cap
is PAYLOAD bytes. Convert only through `worst_case_batch_capacity_bytes` (the factor ALONE
under-states by `SLACK × n_array_nodes`). Never add a payload figure to a capacity figure.**

**Placement rule (design D2, owner-decided): credit is RESERVED BEFORE `rows_to_record_batch`, then
trued up DOWNWARD to the realized capacity. Charging an already-built batch bounds at 16 MiB and is
rejected.**

**Delivered deltas from the authored plan (both applied to the spec/design in the same change):**
1. `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` ships at **12 MiB** (task 2.1 / design D4a as corrected in
   review: 8 MiB clamps every worst-case reservation by 3 permits).
2. The deferred permit is released at the **TOP of the next `poll_next`**, not when the next batch is
   YIELDED (task 1.4). Release-on-next-yield DEADLOCKS a one-batch-deep pool — reachable at the
   DEFAULT ceiling — and the top-of-poll point is strictly tighter anyway, because
   `FlightDataEncoder` re-polls only after dropping the previous batch (arrow-flight 53.4.1
   `encode.rs:400-436`). Verified: `tests/issue_2821_egress_budget_e2e.rs`'s tiny-ceiling test hangs
   under the release-on-yield variant.
3. The `actual > reserved` fail-closed path logs at `tracing::error!` rather than firing a
   `debug_assert!` (task 1.3b): a debug-only abort would turn its own coverage into a panic while
   leaving release builds silent.
4. **File-size ratchet override.** `producer.rs` (3396 → 3431) and `service.rs` (2068 → 2101) are
   already 3–4× over the ~800 campsite target and both grew, so the gate runs with
   `CQLITE_ALLOW_FILE_GROWTH=1`. The growth is the mandated plumbing only — `BatchSink::reserve` +
   the `ProducerError::EgressCredit` variant in `producer.rs`, the `egress_budget` field + builder +
   spawn-site argument in `service.rs` — and every piece of NEW logic went into new files
   (`egress_credit.rs`, `egress_flush.rs`, `metered_stream.rs`, which also took 238 lines OUT of
   `streaming.rs`, 768 → 614). Splitting those two files is epic #1116, out of scope for #2821.

## Stage 0 — fixtures + red tests (write these BEFORE the governor)
- [x] 0.1 REUSE the merged `cqlite-flight/src/wide_row_fixture.rs` (#2825) —
  `wide_row_schema()` + `wide_row_mutations(n_rows, payload_len)` + `FIXTURE_TIMESTAMP`, fed to
  `crate::testutil::build_sstables(&schema, vec![mutations])`. Add any missing shape THERE. Do NOT
  add a second wide-row fixture to `test_fixtures.rs`.
  (flight-streaming-egress: wide-row fixture requirement)
- [x] 0.2 Extend `StreamProbe` (`streaming.rs:99-133`) with charged in-flight CAPACITY observation
  (peak + current), feature-independently, exactly like `produced_batches`. Test-only observation,
  no new OTel metric. (flight-streaming-egress: peak resident payload)
- [x] 0.3 Add the **wide-row byte-ceiling test** in `cqlite-flight/src/streaming_tests.rs`, modelled
  on `slow_consumer_bounds_produced_batches` (`streaming_tests.rs:115`): slow consumer, assert peak
  charged in-flight capacity ≤ `ceiling + max observed batch capacity`. Measured bytes only — **no
  wall-clock threshold assert** (#2642 / `roborev-lints`). Must FAIL on `main`.
  (flight-streaming-egress: peak resident payload)
- [x] 0.4 Add the **narrow-row non-regression test**: at the narrow shape the batch-count channel
  still binds and the produced-batch bound is unchanged.
  (flight-streaming-egress: peak resident payload)
- [x] 0.5 Add the **oversized-batch progress test**: a single batch larger than the whole ceiling is
  still delivered and the stream terminates (a non-clamping implementation hangs).
  (flight-streaming-egress: deadlock-avoidance)
- [x] 0.6 Add the **drop/cancel credit-release test**: after a mid-stream drop the full pool is
  available and nothing wedges. (flight-streaming-egress: credit release)
- [x] 0.7 Add the **deferred-release test**: after exactly one batch is yielded its credit is STILL
  charged, and is released only when the next batch is yielded.
  (flight-streaming-egress: deferred credit release)
- [x] 0.8 Add the **composition test**: `max(DEFAULT_MAX_INFLIGHT_EGRESS_BYTES,
  worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0)) ≤ 16 MiB`, computed
  from the imported constants (no hard-coded `2`, no hard-coded `8 MiB`).
  (flight-streaming-egress: capacity denomination)
- [x] 0.9 Add the **reserve-before-materialize test**: with the pool exhausted the producer parks
  having built NOTHING (no `RecordBatch` materialized while a reservation is pending), and a
  parked-on-reservation producer wakes on cancel. Must FAIL on a charge-at-emit implementation.
  (flight-streaming-egress: reserve before materialize)
- [x] 0.10 Add the **both-loops test**: the ceiling holds through `drive_merge` AND
  `drive_merge_streaming`. (flight-streaming-egress: reserve before materialize)
- [x] 0.11 Add the **true-up-down test** (charged credit tracks realized capacity, not the
  reservation) and the **fail-closed test** (`actual > reserved` terminates with the invariant error
  and leaks no credit). (flight-streaming-egress: true-up downward)

## Stage 1 — the credit governor
- [x] 1.1 Add an `EgressCredit` / `EgressPermit` type (semaphore-backed, permits in KiB, rounding
  UP) plus `EgressBudget` (bounded / unbounded), with `reserve(estimate) → permit` and
  `permit.true_up_down(actual)`. No `unwrap()`/`expect()`.
  (flight-streaming-egress: peak resident payload)
- [x] 1.1a Add an **Arrow array-NODE counter** over the producer's `ArrowSchema` (list = 2, map = 4
  …), computed once per merge — no such helper exists in the tree.
  (flight-streaming-egress: capacity denomination)
- [x] 1.2 Change the egress channel element to a `CreditedBatch` owning the `RecordBatch` + its
  `EgressPermit` (`streaming.rs:300`), so release is RAII on every path. Update the direct
  channel-constructing test helpers in `streaming_tests.rs`.
  (flight-streaming-egress: credit release)
- [x] 1.3 Extend `BatchSink` (`producer.rs:396`) with the pre-materialization reservation step
  (`CollectSink`'s is a no-op) and rework ALL SIX streaming flush sites — `producer.rs:997,1005,1015`
  and `producer_stream.rs:214,222,232` — into ONE owning helper running
  `reserve(worst_case_batch_capacity_bytes(byte_cap.accumulated(), n_array_nodes, 0))` →
  `flush_buffer` → true-up-down → `emit`, so a build site cannot materialize without a reservation.
  `byte_cap.accumulated()` is already exactly the estimate for the rows being flushed at each of the
  six sites — verify that before touching anything else.
  (flight-streaming-egress: reserve before materialize / true-up downward)
- [x] 1.3a Acquire `min(ceil(reservation/KiB), pool_total)` permits INSIDE a biased `select!` that
  races `cancel.cancelled()` (same pattern as `ChannelSink::emit`'s `tx.reserve()`, which runs on a
  `spawn_blocking` thread under `Handle::block_on`). The clamp is the deadlock-avoidance rule —
  comment it as such. (flight-streaming-egress: deadlock-avoidance)
- [x] 1.3b Fail closed when `actual > reserved`: terminal internal error naming the violated
  estimator-conservatism invariant, permit dropped normally, batch NOT emitted. Never true up.
  (flight-streaming-egress: true-up downward)
- [x] 1.3c Document the cross-issue invariant at BOTH ends — a comment at the reservation site
  pointing at `arrow_size_tests.rs`'s property test, and a line in `arrow_size.rs`'s conservatism
  section naming this ceiling as a dependent consumer.
  (flight-streaming-egress: estimator-conservatism dependency)
- [x] 1.4 Release in `MeteredDoGetStream` via a single `deferred: Option<EgressPermit>` slot —
  assigning the next batch's permit drops the previous one, so at most one batch sits downstream of
  the credit boundary (the encoder prefetch, `streaming.rs:381`). `Drop` (`streaming.rs:711`)
  releases the deferred permit. (flight-streaming-egress: deferred credit release)
- [x] 1.5 Verify streams still terminate and byte-parity with the collect path holds; run the whole
  existing `streaming_tests.rs` suite. `--lite` green.
  (flight-streaming-egress: peak resident payload)

## Stage 2 — configuration plumbing (wiring evidence)
- [x] 2.1 Add `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` (**12 MiB of capacity — design D4a as corrected
  in review: the ceiling must clear one worst-case RESERVATION, so 6/8 MiB both clamp**) +
  `ENV_MAX_INFLIGHT_EGRESS_BYTES` (`CQLITE_MAX_INFLIGHT_EGRESS_BYTES`), documenting the derivation
  from `BATCH_BYTES_CAPACITY_FACTOR × DEFAULT_MAX_BATCH_BYTES` against B4 ≤16Mi.
  (flight-streaming-egress: CLI configurability / capacity denomination)
- [x] 2.2 Add `--max-inflight-egress-bytes` to `main.rs` `Args` with
  `#[arg(long, env = …, default_value_t = …)]`, mirroring `--max-batch-bytes` (`main.rs:64-65`);
  log the configured value alongside `max_batch_bytes` (`main.rs:129`).
  (flight-streaming-egress: CLI configurability)
- [x] 2.3 Add the service field + `with_egress_budget` builder mirroring `with_max_batch_bytes`
  (`service.rs:307,343,353`); every construction path applies the default, the builder allows an
  explicit unbounded budget. (flight-streaming-egress: CLI configurability)
- [x] 2.4 Thread the budget through the sole production spawn site (`service.rs:885`) →
  `spawn_streaming_from_readers` (`streaming.rs:260`) → `spawn_streaming` (`streaming.rs:290`) →
  `ChannelSink`. (flight-streaming-egress: CLI configurability)
- [x] 2.5 Add the **end-to-end wiring-evidence test**, following
  `tests/issue_2825_max_batch_bytes_e2e.rs`: the real binary, real clap parse, real `do_get`, a
  configured small ceiling observably governing the stream. A helper-only unit test is NOT
  sufficient. (flight-streaming-egress: CLI configurability)
- [x] 2.6 Add the env-var-backing test and the unbounded-opt-out test.
  (flight-streaming-egress: CLI configurability)

## Stage 3 — documentation corrections (scoped)
- [x] 3.1 Revise the `DO_GET_CHANNEL_CAPACITY` doc comment (`streaming.rs:59-66`): production
  residency ≈ `(4 + 2) × batch_size` ≈ 49,152 rows, flagged row-width dependent; drop the
  `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE` from the production derivation; no 57,344 figure; replace
  "deliberately not a config knob" with a pointer to `--max-inflight-egress-bytes`.
  (flight-streaming-egress: doc-comment requirement)
- [x] 3.2 Document the `max(ceiling, one maximum batch)` contract at the governor's definition site
  in CAPACITY currency, using the SAME derivation the composition test asserts, and NAME the
  residency outside the governed set (the `Vec<QueryRow>` row buffer, an over-cap single row, the
  aggregate route). Do NOT list a parked pre-credit batch — design A eliminated it.
  (flight-streaming-egress: deadlock-avoidance)
- [x] 3.3 Truth up `cqlite-flight/src/batch_bytes.rs`: the module doc's `~7 × 8 MiB ≈ 56 MiB`
  count-bounded paragraph and the "becomes true only once #2821 lands / TARGET for the dependent
  issue" framing (`:66-93`), plus `worst_case_batch_capacity_bytes`'s "Until that ceiling lands…"
  sentence (`:341-342`). Keep the payload-vs-capacity explanation and the published-constant
  conversion. (flight-streaming-egress: #2825 documentation correction)
- [x] 3.4 Correct `docs/flight-trino/JOURNAL.md:659-665` ("B4 composition for issue #2821") from a
  prospective statement to the enforced one, naming this delivery.
  (flight-streaming-egress: #2825 documentation correction)
- [x] 3.5 Confirm NO edits to the dated phase-research docs or
  `docs/architecture/throughput-program-2026-07.md` — out of scope by design.
  (flight-streaming-egress: doc-comment requirement)

## Stage 3b — review round 3 (roborev R1-R5)
- [x] 3b.1 **R1 safety valve**: `MeteredDoGetStream::open_safety_valve` releases the oldest deferred
  permit when the channel is empty AND a reservation is parked NOW (`parked_now` gauge via the RAII
  `ParkGuard`) AND the whole charge is held by consumer-retained batches; the stream registers for
  the producer's next park on its OWN `Notify` before returning `Pending`, so the valve cannot lose
  the race. (flight-streaming-egress: no consumer behaviour can wedge the stream)
- [x] 3b.2 **R1 reframing**: the published bound is stated as **SERVER-SIDE residency** — and
  explicitly NOT as total resident bytes including consumer-held batches — in
  `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES`, the `egress_credit` module doc, `batch_bytes.rs`'s
  composition section, the spec delta, design D2b/D2c, the proposal and the JOURNAL.
- [x] 3b.3 **R1 tests**: `a_retaining_consumer_still_makes_progress` (verified to HANG without the
  valve) + `the_safety_valve_fires_only_when_the_stream_is_wedged` (both directions, deterministic)
  + `safety_valve_releases() == 0` on the two real-encoder drains.
- [x] 3b.4 **R2 corpus**: the shape corpus moves to `cqlite_core::export::arrow_shape_corpus` behind
  the opt-in `arrow-shape-corpus` feature (dev-dependency only for `cqlite-flight`), and
  `the_capacity_bound_holds_over_the_shared_shape_corpus` asserts the published bound over every
  shape at full row count AND at one row. Measured worst case 1188 B/node; FAILS at slack = 1024.
  (flight-streaming-egress: tiny-batch conversion requirement)
- [x] 3b.5 **R3**: `both_producer_loops_reserve_before_materializing` holds the `EgressPermit` for
  as long as the batch (`split()` instead of `into_batch()`) and asserts residency AT THE INSTANT of
  the hold; proven to fail under the old form.
- [x] 3b.6 **R4**: the `--max-inflight-egress-bytes` help text states 12 MiB per stream (not ~8 MiB)
  and the server-side framing.
- [x] 3b.7 **R5**: stale arithmetic swept — the clamp threshold is `n_array_nodes >= 2049` (not
  4097), `~2n KiB` (not `~n KiB`), and the one-maximum-batch figure carries its `+ 2 KiB x nodes`
  term everywhere it appears.

## Stage 4 — gate + audit + review (definition of done)
- [x] 4.1 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code; no wall-clock
  threshold assert in any correctness test.
- [ ] 4.2 rust-reviewer + roborev on the `--lite`-green diff (review-first), blockers fixed with
  `--lite` re-certification per round.
- [ ] 4.3 Full `scripts/agent-gate.sh` ONCE → PASS (SUMMARY recorded via the summary-file redirect).
- [ ] 4.4 spec-auditor **C** anchored to
  `openspec/changes/streaming-egress-byte-budget/specs/**` → PASS (every requirement satisfied with a
  public-surface test).
- [ ] 4.5 Final roborev clean → arm `gh pr merge --auto --squash --delete-branch` after the
  pre-merge SHA assert.
- [ ] 4.6 Close #2821 on merge; archive the change.
