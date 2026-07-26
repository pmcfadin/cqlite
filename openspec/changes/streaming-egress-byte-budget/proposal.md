# Streaming egress byte budget — bound `do_get` residency in BYTES (issue #2821 / M6)

## Milestone
0.17 scan-path throughput program (epic #2817), manifest item **M6**
(`docs/architecture/throughput-program-2026-07.md` §7). **Design-driven** — there is no external
oracle here. Cassandra has no counterpart to CQLite's Flight egress path, so the ceiling, its
default, its configuration surface, and the honesty of the stated bound are all latitude decisions
that need a written contract before code. (Contrast: an SSTable decode bug would be a plain issue +
pinned parity test.)

## Status after #2825
Issue **#2825 (byte-bounded Arrow egress batch sizing, PR #2906) has MERGED**. It caps ONE batch at
`DEFAULT_MAX_BATCH_BYTES = 4 MiB` of Arrow **payload** and publishes
`BATCH_BYTES_CAPACITY_FACTOR = 2` + `worst_case_batch_capacity_bytes(...)` explicitly so this change
can convert that guarantee into the **capacity** currency `streaming.rs` meters. Its own module docs
state that per-stream egress residency is still count-bounded (`~7 × 8 MiB ≈ 56 MiB`) and that the
14 MiB additive composition is a TARGET for this issue. **This change bounds egress for real — and
at a better number than that sketch, because reserve-before-materialize removes the additive term —
so both statements are updated here.**

## Why (measured problem)
Source of truth: `docs/research/phase2-verify-parallelism.md` §2 and the issue.

1. **The 64 MiB `QueryConfig::n` result budget never reaches the streaming path.**
   `result_budget` is enforced by `enforce_result_budget` on the materializing / collect-into-`Vec`
   path only; `rg result_budget cqlite-flight/src` returns nothing. A streaming `do_get` is bounded
   **only** structurally.
2. **The only structural bound is a batch COUNT.** `DO_GET_CHANNEL_CAPACITY = 4`
   (`cqlite-flight/src/streaming.rs:66`) is a 4-deep channel of `RecordBatch`, and the producer
   builds batches to `batch_size` **rows** (default 8192). Peak resident rows/stream is therefore
   `(4 + ~2) × 8192 ≈ 49,152` rows — a **row count**, multiplied by an **unbounded row width**.
3. **So per-stream residency is governed by row width, with no byte ceiling.** At the narrow field
   `keyvalue` shape (~300 B/Arrow-row) that is ~15 MB — already ≈ the whole ratified **B4 ≤16Mi
   per-query working set** at concurrency 1. A wide-row table blows straight through it, and
   nothing in the code names the hazard.
4. **The existing doc comment overstates and mis-derives the bound.** `streaming.rs:59-66` states
   the residency as `(DO_GET_CHANNEL_CAPACITY + IN_FLIGHT_ALLOWANCE) · batch_size`, but
   `IN_FLIGHT_ALLOWANCE = 3` is `#[cfg(test)]`-ONLY (`streaming.rs:85`) and its own doc says it is
   "a test-observation bound, not a value any production code branches on". Only two of its three
   components (+1 send-in-flight, +1 encoder prefetch) are real production residency, so the
   comment implies ~57,344 rows where production is ~49,152 — a ~15% over-count that cites a
   test-only constant as if it were a runtime property. The same comment also declares the depth
   "deliberately not a config knob", which this change supersedes.

The issue's acceptance criteria offer two arms — enforce a byte budget, or document the gap and
make admission K the sole governor. **This change takes the enforce arm.** The document-only arm
would leave per-stream residency proportional to row width forever, which cannot satisfy B4 on a
wide table at any value of K.

## What changes
- **A per-stream in-flight CAPACITY-byte credit governor on the streaming egress.** Credit is
  **reserved at the batch boundary BEFORE the batch is materialized** (design A), from the payload
  estimate #2825's `BatchByteCap` already maintains, converted with
  `worst_case_batch_capacity_bytes`; the realized `get_array_memory_size()` is measured immediately
  after `rows_to_record_batch` and the excess released (**true up DOWNWARD, never upward** — an
  `actual > reserved` is a violated invariant and fails closed). The permit then rides with the
  batch as an RAII `CreditedBatch` and is returned when the batch has left the stream. No
  materialized-but-uncharged `RecordBatch` ever exists on the egress path; per-stream residency
  becomes bounded in BYTES, independent of row width.
- **Why reserve first, not charge at `emit`.** Charging an already-built batch leaves a parked
  producer holding a resident, uncharged batch, which bounds at
  `max(ceiling, max_batch) + max_batch` = 16 MiB at the merged defaults — exactly B4, zero headroom,
  and unfixable by tuning the ceiling because the binding term is `2 × max_batch_capacity`.
  Reserving first deletes that term.
- **A named cross-issue invariant.** This bound now rests on #2825's estimator contract
  (`Σ estimate_arrow_row_bytes >= arrow_payload_bytes`, property-tested) plus
  `capacity <= worst_case_batch_capacity_bytes(payload, n_array_nodes, 0)`. Both ends document the
  dependency so a future weakening cannot silently void the ceiling.
- **Denominated in capacity, converted from #2825 through its published constant.** The per-batch
  cap is payload-denominated; this ceiling is capacity-denominated; the two currencies are named at
  the boundary and converted with `cqlite_flight::batch_bytes::BATCH_BYTES_CAPACITY_FACTOR`, never
  with a locally re-derived factor and never by adding a payload figure to a capacity figure.
- **An explicitly stated, honest bound.** A single batch may exceed the whole ceiling, so the
  governor MUST always admit one batch when nothing else is in flight (otherwise the stream
  deadlocks). The guaranteed contract is therefore **`max(ceiling, one maximum batch)`** — and the
  residency that remains outside the governed set (the producer's `Vec<QueryRow>` row buffer, a
  single row wider than the per-batch cap, the aggregate route) is named rather than implied away.
- **A new configuration knob mirroring the merged `--max-batch-bytes` precedent**:
  `DEFAULT_MAX_INFLIGHT_EGRESS_BYTES` (**12 MiB of capacity**, D4a as corrected in review — the
  smallest round value that admits one worst-case RESERVATION without clamping) + `CQLITE_MAX_INFLIGHT_EGRESS_BYTES`
  env const + a `--max-inflight-egress-bytes` clap arg, plumbed const → `Args` → service field
  (builder mirroring `with_max_batch_bytes`) → the sole production spawn site
  `spawn_streaming_from_readers` → `spawn_streaming` → `ChannelSink` → the producer's reservation
  step (`BatchSink`, both merge loops). On by default on every
  construction path, with an explicit unbounded opt-out for embedders.
- **The composition, in capacity currency**: `max(ceiling, 2 × 4 MiB payload cap + slack) ≈ 8 MiB`
  for any ceiling ≤ 8 MiB — inside the ratified B4 ≤16Mi per-query working set at concurrency 1 with
  ~8 MiB headroom. A test asserts it from the imported constants so neither can drift out from under
  B4. **Owner decision, APPLIED**: the shipped default is 8 MiB — with the additive term gone,
  6 MiB is strictly dominated (same worst case, but at 6 MiB every full-size batch trips the
  deadlock clamp and the stream runs lock-step). See design D4a.
- **Composition, not replacement.** The byte ceiling sits alongside the 4-deep batch-count channel,
  #2825's per-batch cap, and admission K; whichever binds first wins. No existing bound is removed.
- **The `DO_GET_CHANNEL_CAPACITY` doc comment is corrected and revised** to state the real
  production residency (~(4+2)×8192 ≈ 49,152 rows, row-width dependent), to stop citing the
  `#[cfg(test)]` `IN_FLIGHT_ALLOWANCE` as production, and to point at the new byte knob instead of
  claiming the depth is deliberately unconfigurable.
- **#2825's own documentation is truthed up in the same change that makes it stale**:
  `cqlite-flight/src/batch_bytes.rs`'s module docs (the `~56 MiB` count-bounded claim and the
  "14 MiB is a TARGET for #2821" framing) and `docs/flight-trino/JOURNAL.md:659-665`'s prospective
  B4-composition bullet, which the #2906 review deliberately assigned to this issue.

## Non-goals
- **Not** byte-bounded *batch construction* — capping an individual batch is issue **#2825 (T4)**,
  MERGED. This change bounds how many bytes may be in flight; #2825 bounds the one-batch residual
  term. They compose; neither subsumes the other, and this change does not alter #2825's cap, its
  default, or its currency.
- **Not** a change to admission K, its default, its shedding policy, or `--max-concurrent-scans`.
- **Not** a change to the 4-deep `DO_GET_CHANNEL_CAPACITY` value itself (only its doc comment).
- **Not** plumbing `QueryConfig::n` / `result_budget` into `cqlite-flight`. The core result budget
  governs a materialized result set; this is a transport-residency governor with different
  semantics. Reusing the name would be misleading.
- **Not** a change to the aggregate (`aggregate_paths`) route, which is already materialized and
  bounded per-group.
- **Not** a rewrite of the historical phase-research docs. `docs/research/phase2-verify-parallelism.md`
  §2 already records the 49,152-vs-57,344 correction as a finding, and
  `docs/architecture/throughput-program-2026-07.md` M11 was #2825's line. This change touches only
  the doc text its own behaviour falsifies: the `DO_GET_CHANNEL_CAPACITY` comment,
  `batch_bytes.rs`'s residency paragraphs, and the JOURNAL's B4-composition bullet.
- **Not** a new OTel metric. In-flight bytes are exposed through the existing test-only
  `StreamProbe`, consistent with how `produced_batches` is observed today.

## Doctrine impact
- **No-heuristics (#28):** unaffected. `get_array_memory_size()` is an authoritative Arrow-reported
  size, not a byte-pattern inference; no type or format is guessed anywhere in this change.
- **Memory budget:** this change exists to *serve* the <128MB / B4 ≤16Mi posture. With #2825's
  merged 4 MiB payload cap, `max(ceiling, one maximum batch)` is ~8 MiB of capacity at any ceiling
  ≤ 8 MiB — inside B4 ≤16Mi at concurrency 1 with ~8 MiB headroom, and enforced by both governors
  rather than dependent on `batch_size` and row width.
- **Public binding surfaces:** Python/Node/CLI unaffected. The only new public surface is the
  `cqlite-flight` server CLI flag + env var and the `CqliteFlightService` builder.
- **Wiring evidence (#949/#963):** the knob must be reachable end-to-end from the
  `cqlite-flight` server CLI down to the governor, proven by a test that drives a real streamed
  `do_get` — a helper-only unit test on the credit type is NOT sufficient.
- **CLAUDE.md / website `agents-developing/`:** no doctrine text change; this is a server-side
  configuration + memory-bound change with no agent-workflow impact.

## Definition of done
`scripts/agent-gate.sh` full PASS (SUMMARY recorded) + spec-auditor **C** PASS (every requirement
satisfied with a public-surface test) + roborev clean; `RUSTFLAGS="-D warnings"` clean; no
`unwrap()`/`expect()` in library code; no wall-clock threshold assert in any correctness test
(#2642). Then archive.
