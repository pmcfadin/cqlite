# Tasks — Streaming egress byte budget (issue #2821 / M6)

Branch `issue-2821-streaming-result-budget`. Line anchors are `main`-relative at authoring time and
will drift — re-grep before editing. Every stage names the surface it exercises. Tests are written
FIRST and must fail on pre-change `main`.

## Stage 0 — fixtures + red tests (write these BEFORE the governor)
- [ ] 0.1 Add the synthetic **wide-row fixture** to `cqlite-flight/src/test_fixtures.rs`: a wide
  blob / many-column schema + mutation builders + a fixed pinned write timestamp, mirroring the
  `keyvalue_schema` / `keyvalue_write` / `keyvalue_mutations` shape at `test_fixtures.rs:61-108`.
  Self-contained — NO dependence on the fetched `test_wide_rows` dataset.
  (flight-streaming-egress: wide-row fixture requirement)
- [ ] 0.2 Extend `StreamProbe` (`streaming.rs:99-133`) with in-flight-byte observation (peak +
  current), feature-independently, exactly like `produced_batches`. Test-only observation, no new
  OTel metric. (flight-streaming-egress: peak resident payload)
- [ ] 0.3 Add the **wide-row byte-ceiling test** in `cqlite-flight/src/streaming_tests.rs`, modelled
  on `slow_consumer_bounds_produced_batches` (`streaming_tests.rs:115`): slow consumer, assert peak
  in-flight bytes ≤ `ceiling + max observed batch bytes`. Measured bytes only — **no wall-clock
  threshold assert** (#2642 / `roborev-lints`). Must FAIL on `main`.
  (flight-streaming-egress: peak resident payload)
- [ ] 0.4 Add the **narrow-row non-regression test**: at the `keyvalue` shape the batch-count channel
  still binds and the produced-batch bound is unchanged.
  (flight-streaming-egress: peak resident payload)
- [ ] 0.5 Add the **oversized-batch progress test**: a single batch larger than the whole ceiling is
  still delivered and the stream terminates (a non-clamping implementation hangs).
  (flight-streaming-egress: deadlock-avoidance)
- [ ] 0.6 Add the **drop/cancel credit-release test**: after a mid-stream drop the full pool is
  available and nothing wedges. (flight-streaming-egress: credit release)

## Stage 1 — the credit governor
- [ ] 1.1 Add an `EgressCredit` / `EgressPermit` type (semaphore-backed, permits in KiB, rounding
  UP) plus `EgressBudget` (bounded / unbounded). No `unwrap()`/`expect()`.
  (flight-streaming-egress: peak resident payload)
- [ ] 1.2 Change the egress channel element to a `CreditedBatch` owning the `RecordBatch` + its
  `EgressPermit` (`streaming.rs:300`), so release is RAII on every path. Update the direct
  channel-constructing test helpers (`streaming_tests.rs:242,399`).
  (flight-streaming-egress: credit release)
- [ ] 1.3 Charge in `ChannelSink::emit` (`streaming.rs:150-188`): acquire
  `min(ceil(bytes/KiB), pool_total)` permits INSIDE the existing biased `select!` that races
  `cancel.cancelled()`, before `tx.reserve()`. The clamp is the deadlock-avoidance rule — comment it
  as such. (flight-streaming-egress: deadlock-avoidance)
- [ ] 1.4 Release in `MeteredDoGetStream` via a single `deferred: Option<EgressPermit>` slot —
  assigning the next batch's permit drops the previous one, so at most one batch sits downstream of
  the credit boundary (the encoder prefetch). `Drop` (`streaming.rs:711`) releases the deferred
  permit. (flight-streaming-egress: credit release)
- [ ] 1.5 Verify streams still terminate and byte-parity with the collect path holds; run the whole
  existing `streaming_tests.rs` suite. `--lite` green.
  (flight-streaming-egress: peak resident payload)

## Stage 2 — configuration plumbing (wiring evidence)
- [ ] 2.1 Add `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` (8 MiB) + `ENV_MAX_INFLIGHT_EGRESS_BYTES`
  (`CQLITE_MAX_INFLIGHT_EGRESS_BYTES`), mirroring `admission.rs:43,51`.
  (flight-streaming-egress: CLI configurability)
- [ ] 2.2 Add `--max-inflight-egress-bytes` to `main.rs` `Args` (`main.rs:25`) with
  `#[arg(long, env = …, default_value_t = …)]`, mirroring `--max-concurrent-scans` at
  `main.rs:43-44`; log the configured value alongside `max_concurrent_scans` (`main.rs:112`).
  (flight-streaming-egress: CLI configurability)
- [ ] 2.3 Add the service field + `with_egress_budget` builder mirroring `with_admission`
  (`service.rs:286-334`); `new()` applies the default, the builder allows an explicit unbounded
  budget. (flight-streaming-egress: CLI configurability)
- [ ] 2.4 Thread the budget through the production spawn site (`service.rs:857-863`) →
  `spawn_streaming_from_readers` (`streaming.rs:260`) → `spawn_streaming` (`streaming.rs:290`) →
  `ChannelSink`. (flight-streaming-egress: CLI configurability)
- [ ] 2.5 Add the **end-to-end wiring-evidence test**: a service built the way `main` builds it,
  with a configured small ceiling, observably governs a real streamed `do_get`. A helper-only unit
  test is NOT sufficient. (flight-streaming-egress: CLI configurability)
- [ ] 2.6 Add the env-var-backing test and the unbounded-opt-out test.
  (flight-streaming-egress: CLI configurability)

## Stage 3 — documentation correction (scoped)
- [ ] 3.1 Revise the `DO_GET_CHANNEL_CAPACITY` doc comment (`streaming.rs:59-66`): production
  residency ≈ `(4 + 2) × batch_size` ≈ 49,152 rows, flagged row-width dependent; drop the
  `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE` from the production derivation; no 57,344 figure; replace
  "deliberately not a config knob" with a pointer to `--max-inflight-egress-bytes`.
  (flight-streaming-egress: doc-comment requirement)
- [ ] 3.2 Document the `ceiling + one maximum batch` contract and the #2825 follow-on at the
  governor's definition site, using the SAME derivation the ceiling test asserts so the two cannot
  drift. (flight-streaming-egress: deadlock-avoidance)
- [ ] 3.3 Confirm NO edits to `docs/research/phase1-6-parallelism.md`,
  `docs/research/phase2-verify-row-engine.md`, `docs/research/phase2-verify-transport.md`,
  `docs/research/phase2-verify-parallelism.md`, or
  `docs/architecture/throughput-program-2026-07.md` — out of scope by design (M11/#2825 owns its own
  line). (flight-streaming-egress: doc-comment requirement)

## Stage 4 — gate + audit + review (definition of done)
- [ ] 4.1 `RUSTFLAGS="-D warnings"` clean; no `unwrap()`/`expect()` in library code; no wall-clock
  threshold assert in any correctness test.
- [ ] 4.2 rust-reviewer + roborev on the `--lite`-green diff (review-first), blockers fixed with
  `--lite` re-certification per round.
- [ ] 4.3 Full `scripts/agent-gate.sh` ONCE → PASS (SUMMARY recorded via the summary-file redirect).
- [ ] 4.4 spec-auditor **C** anchored to
  `openspec/changes/streaming-egress-byte-budget/specs/**` → PASS (every requirement satisfied with a
  public-surface test).
- [ ] 4.5 Final roborev clean → arm `gh pr merge --auto --squash --delete-branch` after the
  pre-merge SHA assert.
- [ ] 4.6 Close #2821 on merge; archive the change. Confirm #2825 (T4 byte-bounded batch sizing) is
  recorded as the follow-on that caps the one-batch residual term.
