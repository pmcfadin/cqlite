# Tasks — Streaming egress byte budget (issue #2821 / M6)

Branch `issue-2821-streaming-result-budget`. Line anchors are `main`-relative post-#2825 and will
drift — re-grep before editing. Every stage names the surface it exercises. Tests are written FIRST
and must fail on pre-change `main`.

**Currency rule (design D0): the ceiling is CAPACITY bytes (`get_array_memory_size()`); #2825's cap
is PAYLOAD bytes. Convert only through `cqlite_flight::batch_bytes::BATCH_BYTES_CAPACITY_FACTOR` /
`worst_case_batch_capacity_bytes`. Never add a payload figure to a capacity figure.**

## Stage 0 — fixtures + red tests (write these BEFORE the governor)
- [ ] 0.1 REUSE the merged `cqlite-flight/src/wide_row_fixture.rs` (#2825) —
  `wide_row_schema()` + `wide_row_mutations(n_rows, payload_len)` + `FIXTURE_TIMESTAMP`, fed to
  `crate::testutil::build_sstables(&schema, vec![mutations])`. Add any missing shape THERE. Do NOT
  add a second wide-row fixture to `test_fixtures.rs`.
  (flight-streaming-egress: wide-row fixture requirement)
- [ ] 0.2 Extend `StreamProbe` (`streaming.rs:99-133`) with charged in-flight CAPACITY observation
  (peak + current), feature-independently, exactly like `produced_batches`. Test-only observation,
  no new OTel metric. (flight-streaming-egress: peak resident payload)
- [ ] 0.3 Add the **wide-row byte-ceiling test** in `cqlite-flight/src/streaming_tests.rs`, modelled
  on `slow_consumer_bounds_produced_batches` (`streaming_tests.rs:115`): slow consumer, assert peak
  charged in-flight capacity ≤ `ceiling + max observed batch capacity`. Measured bytes only — **no
  wall-clock threshold assert** (#2642 / `roborev-lints`). Must FAIL on `main`.
  (flight-streaming-egress: peak resident payload)
- [ ] 0.4 Add the **narrow-row non-regression test**: at the narrow shape the batch-count channel
  still binds and the produced-batch bound is unchanged.
  (flight-streaming-egress: peak resident payload)
- [ ] 0.5 Add the **oversized-batch progress test**: a single batch larger than the whole ceiling is
  still delivered and the stream terminates (a non-clamping implementation hangs).
  (flight-streaming-egress: deadlock-avoidance)
- [ ] 0.6 Add the **drop/cancel credit-release test**: after a mid-stream drop the full pool is
  available and nothing wedges. (flight-streaming-egress: credit release)
- [ ] 0.7 Add the **deferred-release test**: after exactly one batch is yielded its credit is STILL
  charged, and is released only when the next batch is yielded.
  (flight-streaming-egress: deferred credit release)
- [ ] 0.8 Add the **composition test**: `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES +
  worst_case_batch_capacity_bytes(DEFAULT_MAX_BATCH_BYTES, n_array_nodes, 0) ≤ 16 MiB`, computed
  from the imported constants (no hard-coded `2`, no hard-coded `8 MiB`).
  (flight-streaming-egress: capacity denomination)

## Stage 1 — the credit governor
- [ ] 1.1 Add an `EgressCredit` / `EgressPermit` type (semaphore-backed, permits in KiB, rounding
  UP) plus `EgressBudget` (bounded / unbounded). No `unwrap()`/`expect()`.
  (flight-streaming-egress: peak resident payload)
- [ ] 1.2 Change the egress channel element to a `CreditedBatch` owning the `RecordBatch` + its
  `EgressPermit` (`streaming.rs:300`), so release is RAII on every path. Update the direct
  channel-constructing test helpers in `streaming_tests.rs`.
  (flight-streaming-egress: credit release)
- [ ] 1.3 Charge in `ChannelSink::emit` (`streaming.rs:149-188`): acquire
  `min(ceil(capacity_bytes/KiB), pool_total)` permits INSIDE the existing biased `select!` that
  races `cancel.cancelled()`, before `tx.reserve()`. The clamp is the deadlock-avoidance rule —
  comment it as such. (flight-streaming-egress: deadlock-avoidance)
- [ ] 1.4 Release in `MeteredDoGetStream` via a single `deferred: Option<EgressPermit>` slot —
  assigning the next batch's permit drops the previous one, so at most one batch sits downstream of
  the credit boundary (the encoder prefetch, `streaming.rs:381`). `Drop` (`streaming.rs:711`)
  releases the deferred permit. (flight-streaming-egress: deferred credit release)
- [ ] 1.5 Verify streams still terminate and byte-parity with the collect path holds; run the whole
  existing `streaming_tests.rs` suite. `--lite` green.
  (flight-streaming-egress: peak resident payload)

## Stage 2 — configuration plumbing (wiring evidence)
- [ ] 2.1 Add `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` (**6 MiB of capacity**) +
  `ENV_MAX_INFLIGHT_EGRESS_BYTES` (`CQLITE_MAX_INFLIGHT_EGRESS_BYTES`), documenting the derivation
  from `BATCH_BYTES_CAPACITY_FACTOR × DEFAULT_MAX_BATCH_BYTES` against B4 ≤16Mi.
  (flight-streaming-egress: CLI configurability / capacity denomination)
- [ ] 2.2 Add `--max-inflight-egress-bytes` to `main.rs` `Args` with
  `#[arg(long, env = …, default_value_t = …)]`, mirroring `--max-batch-bytes` (`main.rs:64-65`);
  log the configured value alongside `max_batch_bytes` (`main.rs:129`).
  (flight-streaming-egress: CLI configurability)
- [ ] 2.3 Add the service field + `with_egress_budget` builder mirroring `with_max_batch_bytes`
  (`service.rs:307,343,353`); every construction path applies the default, the builder allows an
  explicit unbounded budget. (flight-streaming-egress: CLI configurability)
- [ ] 2.4 Thread the budget through the sole production spawn site (`service.rs:885`) →
  `spawn_streaming_from_readers` (`streaming.rs:260`) → `spawn_streaming` (`streaming.rs:290`) →
  `ChannelSink`. (flight-streaming-egress: CLI configurability)
- [ ] 2.5 Add the **end-to-end wiring-evidence test**, following
  `tests/issue_2825_max_batch_bytes_e2e.rs`: the real binary, real clap parse, real `do_get`, a
  configured small ceiling observably governing the stream. A helper-only unit test is NOT
  sufficient. (flight-streaming-egress: CLI configurability)
- [ ] 2.6 Add the env-var-backing test and the unbounded-opt-out test.
  (flight-streaming-egress: CLI configurability)

## Stage 3 — documentation corrections (scoped)
- [ ] 3.1 Revise the `DO_GET_CHANNEL_CAPACITY` doc comment (`streaming.rs:59-66`): production
  residency ≈ `(4 + 2) × batch_size` ≈ 49,152 rows, flagged row-width dependent; drop the
  `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE` from the production derivation; no 57,344 figure; replace
  "deliberately not a config knob" with a pointer to `--max-inflight-egress-bytes`.
  (flight-streaming-egress: doc-comment requirement)
- [ ] 3.2 Document the `ceiling + one maximum batch` contract at the governor's definition site in
  CAPACITY currency, using the SAME derivation the composition test asserts, and NAME the terms
  outside the governed set (the producer's parked pre-credit batch, an over-cap single row, the
  per-node slack). (flight-streaming-egress: deadlock-avoidance)
- [ ] 3.3 Truth up `cqlite-flight/src/batch_bytes.rs`: the module doc's `~7 × 8 MiB ≈ 56 MiB`
  count-bounded paragraph and the "becomes true only once #2821 lands / TARGET for the dependent
  issue" framing (`:66-93`), plus `worst_case_batch_capacity_bytes`'s "Until that ceiling lands…"
  sentence (`:341-342`). Keep the payload-vs-capacity explanation and the published-constant
  conversion. (flight-streaming-egress: #2825 documentation correction)
- [ ] 3.4 Correct `docs/flight-trino/JOURNAL.md:659-665` ("B4 composition for issue #2821") from a
  prospective statement to the enforced one, naming this delivery.
  (flight-streaming-egress: #2825 documentation correction)
- [ ] 3.5 Confirm NO edits to the dated phase-research docs or
  `docs/architecture/throughput-program-2026-07.md` — out of scope by design.
  (flight-streaming-egress: doc-comment requirement)

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
- [ ] 4.6 Close #2821 on merge; archive the change.
