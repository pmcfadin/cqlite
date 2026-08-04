# Tasks: arrow-encode-doget (issue #3096)

## DELIVERY STATUS — LEVER 6 IS REVERTED (owner ruling, 2026-08-04)

**Lever 6 — caching the egress Arrow schema once per merge instead of building it
per batch — is REVERTED IN FULL.** `EgressBatchPlan` / `egress_batch_plan()` are
gone; `egress_array_nodes() -> usize` and the bare `n_array_nodes: usize` parameter
`flush_credited` takes at all six flush sites are restored; `flush_buffer` calls
`rows_to_record_batch(&self.columns, buffer)` per batch as `origin/main` does. The
owner's rule: **a lever that measures zero with its mechanism genuinely in effect
has no rationale and is reverted whole.**

**MEASURED TWICE, ZERO BOTH TIMES.** The two readings are not a repeat — the first
was not a result about lever 6 at all:

| reading | value | what it actually measured |
|---|---|---|
| first (2026-08-03, `abc-interleaved-2026-08-03.md` §10) | **−496 rows/s (−0.2%)** | **NOT a result about the lever.** A redundant per-batch `check_schema_matches_columns` on the egress path reconstructed a `Field` per column per batch, re-adding under another name exactly the work the cache removed. So this measured **lever 6 PLUS a bug**, with the mechanism defeated. |
| second (2026-08-04, post-`035585d`) | **+732 rows/s (+0.30%)**, 95% CI **[−1661, +3125]** covering zero, 8/12 rounds positive | lever 6 **with the mechanism live** — `035585d` routed `do_get`'s flush through a trusted tail, so the schema was genuinely built once per merge. |

Supporting figures for the second reading: ON (HEAD `035585d`) 244,895 rows/s at
1.9% spread vs OFF (lever 6 reverted whole) 243,588 rows/s at 4.3%; cycles/row mean
−59 but **MEDIAN +44 — the wrong sign**; the between-binary code-layout noise floor
on this box is **~1.4%**, so **+0.30% is 4.5x BELOW noise**. The per-batch work
removed measures **1,475 ns over 709 batches = 0.261 ns/row = 1.53 cycles/row of
23,940 (0.0064%)**, which is why no rig on this box can see it.

**Both levers are now reverted** (lever 4 by ruling A below, lever 6 here). No AC
verdict changes:

* **AC1 stays `unmet`, re-anchored to #3248** — lever 6 measured at zero, so
  removing it moves no throughput claim.
* **R4 stays `unmet`, re-anchored to #3272** — the rig split is a separate ruling.
* **No recorded measurement, figure, ratio or superseded-figure label is altered by
  this revert.** Both readings above are the honest record, and they are precisely
  what justifies removing the lever.

### The roborev schema-reuse finding is MOOT, not closed

The finding's subject was that **a redundant per-batch validation defeated the
schema-reuse optimisation**. Reverting removes the optimisation, so **the subject
ceases to exist** — the defect is not repaired, it is **MOOT**. That distinction is
load-bearing: nothing here claims a validation bug was fixed by a revert.

For the record, `035585d` DID close it on the shipping path first (a
`PrevalidatedSchema` whose type made a schema/columns mismatch unconstructible, so
`do_get`'s flush revalidated nothing) — and it is precisely that fix which made the
second, honest measurement possible. With lever 6 gone that type has no caller, so
it is **dropped rather than carried forward as dead API**: `arrow_prevalidated.rs`,
`rows_to_record_batch_prevalidated`, the `arrow-validation-probe` feature and
`prevalidated_batch_builds_on_this_thread` are all removed, along with
`egress_flush_tests.rs` (all four of its tests had lever 6 as their subject).

**What IS retained from that work, on its own merit:**

* `rows_to_record_batch` does not revalidate a schema it just built (`9e01a42`).
  This serves every public caller plus `producer.rs:963`'s aggregate route, is
  independent of lever 6, and keeps its falsifying counter test
  (`the_trusted_path_does_not_revalidate_and_the_external_one_still_does`), whose
  thread-local returns to `#[cfg(test)]`-only.
* `rows_to_record_batch_with_schema`'s **`Field`-identity rejection contract** —
  name, data type, nullability, field metadata, arity, order, empty schema-level
  metadata — with its eleven per-axis tests and the no-false-rejection complement.
  Its docs no longer advertise schema hoisting as a performance route; they record
  the measured negative.

**The egress credit / byte-cap contract (spec R6) is UNTOUCHED.** It consumes only
the array-node count, and the per-merge hoist of THAT predates this branch
(`origin/main`'s `egress_array_nodes()`, issue #2821) and is not lever 6.
`EgressBatchPlan` existed only as the vehicle for the cached schema, so with the
schema gone a one-field struct wrapping a `usize` is dead API and the bare scalar is
restored. `worst_case_batch_capacity_bytes`, the reserve → build → true-up-DOWNWARD
→ emit ordering, the debug accumulator ⇄ buffer invariant, mid-stream cancellation
and the `StreamSubPhase::Encode` span are unchanged; `issue_2821_egress_budget_e2e`
and `issue_2825_max_batch_bytes_e2e` are green.

**What the PR still delivers**, after both reverts: the `arrow_convert.rs`
responsibility split; the `Field`-identity schema contract plus the
non-revalidating trusted tail; the **IPC-framing attribution (313.0 ns/row**,
previously attributable to nothing); the **Arrow-buffer digest oracle, now with a
producer-side tap and real validity-bitmap coverage** (150 nulls over 500 rows, so a
misplaced validity bit has something to misplace); and the **honest negative
result**.

### The measurement lesson

**A same-binary drift control understates the noise floor for a between-binary
A/B.** The ~1.4% code-layout floor was invisible to CTRL (same binary, so no layout
difference to see) and was only exposed by a third arm that measured **faster while
doing strictly more work** — an outcome no same-binary control can produce.

---

## DELIVERY STATUS — LEVER 4 IS REVERTED (owner ruling A, 2026-08-03)

**Lever 4 — the explicit `with_max_flight_data_size` flight-data target, plus the
`wire_partition` byte-partitioner and serialized-message ceiling guard built to
make it a bound — is REVERTED IN FULL. Deferred to issue #3281.** The whole
surface is gone: `cqlite-flight/src/flight_data_size.rs`, `wire_partition.rs`,
`wire_partition_tests.rs`, `streaming_framing_tests.rs`, the 4 MiB-rejection
constants with their `const _: () = assert!` guards, and `prost` as a
`[dependencies]` entry. `encode_do_get` inherits arrow-flight 53's own 2 MiB
target again — the configuration that was shipping before this change and was
never reported broken.

| | |
|---|---|
| lever 4 throughput | **ZERO, measured** — median −72 rows/s (−0.03%), 4 of 8 interleaved rounds positive (`abc-interleaved-2026-08-03.md` §10) |
| its only retained justification | **wire safety** — and that mechanism failed **three consecutive reviews**, each fix inverting the error |
| **lever 4 verdict** | **REVERTED** — deferred to **#3281** |

**Why, in one sentence: ONE NUMBER SERVED AS BOTH A `target` AND A `ceiling`.**
That is the owner's diagnosis and it is a single DESIGN error, not three bugs. "A
larger reserve can only cause extra splitting, which is the safe direction" is
true of a *target* and **false of a *ceiling***: measured against a target, an
over-estimated reserve merely splits more; measured against a ceiling,
**under-budget gives a false-ACCEPT (an illegal message reaches the client) and
over-budget gives a false-REJECT on LEGAL input** (`RowTooWide` on a row a client
could legally receive). Three review rounds each moved the error from one side of
that inversion to the other, because no reserve value can be correct for both
roles at once. **Reverting is subtractive** and needs no new mechanism to be
believed.

**The AC framing, in the owner's words: "we measured it at zero AND did not retain
it."** That is a cleaner honest negative than shipping a lever for a safety
property whose mechanism failed three attempts. It changes no AC verdict:

* **AC1 stays `unmet`, re-anchored to #3248** (unchanged — lever 4 measured at
  zero, so removing it moves no throughput claim).
* **R4 stays `unmet`, re-anchored to #3272** (unchanged — the rig split is a
  separate ruling).
* **No recorded measurement, figure, ratio or superseded-figure label is altered
  by the revert.** The §10 arms, medians and −0.03% are the honest record of what
  was measured, and they are precisely what justifies removing it. The one
  artifact statement the revert falsifies — §10.5's "what lever 4 is retained for
  is WIRE SAFETY" — is struck in place rather than deleted, so the record stays
  legible.

### The two open review findings are DELETED WITH THEIR SUBJECT — explicitly NOT waived

Both lived ONLY in files this revert deletes, so there is no remaining code for
either to be true of. Neither is dismissed, downgraded, or accepted as risk; each
is recorded here with the successor issue so that a re-introduction under **#3281**
must answer it before it lands:

1. **The over-estimated reserve subtracted from a HARD ceiling caused `RowTooWide`
   on legal input.** Lived in `wire_partition.rs` (`guard_message_within_ceiling` /
   the partitioner's ceiling arithmetic) — deleted. This *is* the target-vs-ceiling
   design error, observed as a defect. **Carried to #3281 as a design constraint any
   future wire-ceiling mechanism must satisfy: a ceiling needs an EXACT bound, not a
   reserve, and a false-reject on legal input is worse than the splitting it avoids.**
2. **`ipc_header_bytes` built a fresh `DictionaryTracker` per call.** Lived in
   `wire_partition.rs` — deleted. **Carried to #3281**: any successor that measures a
   serialized message size must not re-derive per-call IPC state per message.

---

## DELIVERY STATUS — R4 is UNMET (owner-ordered split, 2026-08-03)

**R4 ("The corpus generator and measurement scripts are COMMITTED and runnable from
a clean checkout"): UNMET. Re-anchored to issue #3272.** Stated in the same plain
form as AC1/R1 below, with no optimistic framing:

| | |
|---|---|
| what R4 requires in this PR | a committed corpus generator + committed measurement scripts, runnable from a clean checkout |
| what this PR now ships | **neither** — `tools/ws0-corpus-gen/` and `scripts/perf/` were REMOVED from this branch |
| **R4 verdict** | **UNMET** — re-anchored to **#3272** |

**Why.** A sliced roborev pass (6 rounds, every one `prompt-content: PASS`) left 7
blockers open at HEAD, **5 of them in `scripts/perf/`** — three being guards that
earlier fix rounds had made fail-open or bypassable. The measurement rig needs its
own review footing and must not hold the reviewed core (the `arrow_*` export split,
the `Field`-identity validation, the IPC-framing attribution — and, at the time,
the wire-partition/framing work, since reverted per ruling A above). The owner
therefore ordered the rig out of this PR and into **#3272**.

**R4's spec text is deliberately NOT edited or softened** — the requirement stands
in `specs/arrow-encode-doget/spec.md` exactly as written, and this block records its
delivery status. Removed here, delivered there:

* `tools/ws0-corpus-gen/` (generator binary, bare-scan bench, corpus-identity
  recorder, and the generator self-check tests);
* `scripts/perf/` (driver, CPU-sibling-pinning library, reporter, README) and
  `scripts/tests/test_ws0_report_guards.sh` with its `agent-gate.sh` hook.

**What survives here, and is unaffected:** every artifact under
`docs/reports/ws0-3096-artifacts/` (the posted evidence R5 requires — no recorded
measurement, figure, superseded-figure label or AC1 statement is changed by the
split), the method doc, and the in-repo Arrow-buffer digest oracle.

**R3 status after the split: satisfied, with one arm now rig-dependent.** The
oracle's **CI-fixture** arm is fully self-contained — its fixture is written in-test
by the new `cqlite-flight/tests/support/ws0_fixture.rs` support module (same schema,
same row synthesis, same write path), and its pinned digest is UNCHANGED at
`0xd001_4e42_e893_f87f` over 500 rows in 4 batches, identical on the `bypass` and
`merge` arms. The oracle's opt-in **measurement-corpus** arm (`CQLITE_WS0_CORPUS_DIR`)
still runs against any such corpus, but the generator that PRODUCES one is in #3272,
so with the env var unset it SKIPs with an explicit reason naming #3272 rather than
failing or passing vacuously. A corpus dir that is SET but unusable remains a hard
failure.

---

## DELIVERY STATUS — AC1 is UNMET (owner-ruled ship, 2026-08-03)

**AC1 (`do_get rows/s >= same-session bare scan / 1.3`): UNMET. Re-anchored to
issue #3248.** Stated plainly, with no optimistic framing:

| | |
|---|---|
| delivered throughput | **ZERO, measured** — see the re-measurement row below |
| ratio (shipped target, re-measured session) | **1.553x** (target 1.3x) |
| AC1 target on that session's control | **275,223 rows/s** (control median 357,790) |
| shortfall | **−44,902 rows/s = 16.3% short** |
| **AC1 verdict** | **UNMET** — re-anchored to **#3248**; lever 1 routes to **#3231** |

**RE-MEASURED after the review's wire-safety fix (2026-08-03, 8 rounds / 3 arms /
24 runs — `abc-interleaved-2026-08-03.md` §10).** The review found the
flight-data target sitting exactly ON `GRPC_DEFAULT_MAX_MESSAGE_BYTES` behind a
`<=` guard that admitted it; the target moved to 4,063,232 B (ceiling − 64 KiB
framing reserve − 64 KiB inexactness margin). At the shipped target:

| | |
|---|---|
| lever 4 (isolated, `L4P` − `NOTGT`) | **−72 rows/s (−0.03%)**, 4/8 rounds positive — **measured at ZERO** |
| cumulative (`L4P` − `BASE`) | **−573 rows/s (−0.25%)**, 3/8 rounds positive — also zero |
| the SUPERSEDED figure | +2.0% / 213,471 → 217,791 rows/s was measured AT the 4 MiB target and is **not** the delivered figure |
| cycles/row | lever 4' cuts a median 136.9 cycles/row (~0.6%) with rows/s unmoved — **spec R1 forbids reporting that as a win**, and it is not |

**So BOTH levers measured at zero at the shipped target**, and BOTH are now
reverted — see the two REVERTED blocks at the top of this file:

* **Lever 4: measured at zero AND NOT RETAINED.** It is REVERTED in full (owner
  ruling A, deferred to **#3281**). It was briefly retained for *wire safety*,
  which this file previously recorded; that justification is withdrawn, because the
  mechanism behind it conflated a `target` with a `ceiling` and failed three
  consecutive reviews.
* **Lever 6: measured at zero AND NOT RETAINED.** ~~measured at zero and RETAINED,
  on the narrow ground that it is strictly less work per batch~~ — struck in place
  rather than deleted, so the record stays legible. That "strictly less work per
  batch" ground **was not true as written while the redundant validation stood**,
  and once `035585d` made it true the re-measurement was still **+0.30% with a CI
  covering zero, 4.5x below the ~1.4% between-binary noise floor**. Measured twice,
  zero both times → **REVERTED in full** (owner ruling, 2026-08-04).

**Spec R5 (owner-approved) makes a correctly-measured, correctly-reported
negative result a satisfying outcome of THIS change. It does not make AC1
satisfied, and nothing in this tree may say otherwise.** We do not launder a
negative result through an optimistic title. The **C** intent audit should record
**AC1: unmet**; the spec has deliberately NOT been edited to soften it.

**What the change delivers instead, all of it verifiable** (as re-scoped by the
2026-08-03 split: the reproduction rig that produced the numbers below —
`tools/ws0-corpus-gen` + `scripts/perf/` — is **re-anchored to #3272** and is no
longer part of this PR; its ARTIFACTS remain committed here): the in-repo
Arrow-buffer digest oracle — now with a PRODUCER-SIDE tap and real validity-bitmap
coverage — the closed IPC-framing attribution blind spot (**313.0 ns/row**,
previously attributable to nothing), the `arrow_*` export split with its
`Field`-identity schema contract and non-revalidating trusted tail, the
cross-session drift finding — observed TWICE now — and the honest 16.3% gap with
its per-run evidence. **NEITHER lever is in that list: both were measured at zero
and NOT retained** (lever 4 reverted → #3281; lever 6 reverted → see the block at
the top of this file, measured at zero TWICE).

**The 82% is a COMPLEMENT, not an attribution** (`1,746 − 313 = 1,432.9 ns/row`,
labeled "array build" from the call graph, **no per-function data inside it**).
Levers 2, 3 and 1 are three unpriced candidates in one undifferentiated bucket.
No prose here or anywhere says a lever "dominates the 82%".

Evidence: `docs/reports/ws0-3096-artifacts/abc-interleaved-2026-08-03.md` (+ the
per-run `abc-interleaved-runs.json`), `baseline-2026-08-03.md` (read its drift
annotation), `measurement-method.md` §3b.

Per the owner ruling, **levers 1/2/3/5/7 were deliberately NOT implemented** —
§§3–5 below stay unchecked on purpose. The kill criterion (spec R5) fired and was
honored: no further lever was stacked on an unexplained result.

---

> **Seam-1 owner decisions — ANSWERED (attended rulings 2026-08-03).** The four questions
> (see `design.md` §"Owner decisions needed at Seam 1") resolved as: (1) the rig lives in
> `tools/ws0-corpus-gen` + `scripts/perf/` with the artifacts under
> `docs/reports/ws0-3096-artifacts/`; (2) yes — the `arrow_convert.rs` split is a precursor commit
> in this PR; (3) lever 5 is **dropped** (wire blast radius not justified by the remaining gain);
> (4) moot — the cheap levers did **not** reach the ratio, and the ruling was to **ship the rig +
> lever 4 + the framing attribution + the honest null result and stop**, not to chase the 1.3x.
>
> *Original wording, retained:* "(1) where the committed rig lives; (2) whether the
> `arrow_convert.rs` file-size split is a precursor commit in this PR; (3) lever 5's wire blast
> radius; (4) whether to stop early if the cheap levers already reach the ratio. **Do not start
> section 2 before these are answered.**"
>
> **Owner-approved scope carried in (attended decision 2026-08-03):** the reproduction rig named in
> the issue (`/home/ubuntu/ws0-local/`) does NOT exist and was never committed, so this change ALSO
> reconstitutes and COMMITS the corpus generator, the measurement scripts, and an in-repo digest
> oracle. AC1 is re-pinned to its RATIO form; the absolutes 240,100 / 312,155 rows/s are NOT
> thresholds.
>
> Binding constraints: acceptance is an external rows/s ratio, never a CPU-share shift (AC2); the
> two measurement traps are mandatory (AC3); output invariance is proven by the in-repo digest, not
> the absent `0x0a2a390223bde6aa` (AC4); warm and cold stay separate (AC5); the kill criterion is
> binding and a correctly reported negative result IS a satisfying outcome (AC6).

## 0. Commit the rig and re-baseline (BLOCKING — gates every acceptance claim)
- [x] Record in the PR that `/home/ubuntu/ws0-local/` is absent, that `docs/reports/ws0-3026-artifacts/
      ws0-cqlite/scan-harness/Cargo.toml` points at the dead path `/home/ubuntu/workspace/wt-3026/
      cqlite-core`, and that the #3100 corpus is Cassandra-written + LZ4 — so no #3096 number is
      currently reproducible on this box.
- [ ] **RE-ANCHORED TO #3272 (R4 unmet here).** **Corpus generator** (surface: new `tools/ws0-corpus-gen` binary): drives the production
      `cqlite_core` `SSTableWriter` from the pinned `ws0.events` DDL
      (`docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql`); 4,000,000 rows as
      40,000 partitions x 100 rows; partitions emitted in Murmur3 token order; **uncompressed**
      (assert no `CompressionInfo.db` is written — #1406); deterministic from a recorded seed.
- [ ] **RE-ANCHORED TO #3272 (R4 unmet here).** Generator self-checks: re-running with the same seed produces a byte-identical `Data.db`;
      writing/observing 0 rows exits non-zero; the emitted row/partition counts are asserted, not
      assumed. (The equivalent anti-vacuity checks for the CI FIXTURE — non-zero rows, confirmed
      partition count, non-empty `Data.db`, no `CompressionInfo.db` — remain in
      `cqlite-flight/tests/support/ws0_fixture.rs` and are asserted by the digest oracle.)
- [x] Record the corpus identity in-tree: `sha256`, row count, cells/row, on-disk bytes, bytes/row.
      State explicitly that it differs from #3058's `0185909de6da…` by construction and that the
      old digest is NOT asserted.
- [x] Document the corpus as a **PERFORMANCE FIXTURE ONLY** in the generator's own docs — a
      CQLite-written + CQLite-read corpus is invariant to a uniform framing error (#3042) and is
      never a correctness oracle for on-disk framing. (Carried into
      `cqlite-flight/tests/support/ws0_fixture.rs`, which states the same scope for the CI
      fixture; `docs/reports/ws0-3096-artifacts/measurement-method.md` states it for the corpus.)
- [ ] **RE-ANCHORED TO #3272 (R4 unmet here).** **Measurement scripts** (surface: `scripts/perf/…` + method doc under `docs/reports/`):
      CPU-wide `perf stat -C <cpu-list>`, **no** `perf stat -p` anywhere; `taskset` pinning to a
      pair read from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list` that **fails
      closed** if the pair is not siblings of one physical core; median of 3 with spread; setup
      subtracted from the cycles/row denominator; row denominator printed with every figure.
- [x] Both arms in ONE session on ONE pinned pair: bare scan via `execute_streaming`, Flight
      `do_get` over a real loopback transport. Warm and cold as separate runs, reported separately.
      (EXECUTED, and its artifacts stay committed; the DRIVER that runs it is re-anchored to
      #3272, so re-running it from this branch alone is not possible — that is R4's gap, recorded
      above.)
- [x] **In-repo Arrow-buffer digest oracle** (surface: `cqlite-flight/tests/`) — KEPT, and now
      self-contained: its CI fixture is written in-test by
      `cqlite-flight/tests/support/ws0_fixture.rs`, and the pinned digest is unchanged. Folds each emitted
      `RecordBatch`'s value + validity buffers in column order plus row count; asserts equality
      across `CQLITE_FLIGHT_MERGE_PATH=bypass|merge` at a PINNED `now`, plus row count and
      cells/row. Shares `PROBE_LOCK` with `issue_3058_forced_path_differential.rs` (process-global
      env). One `#[test]`, cases in a list.
- [x] Close the attribution blind spot: `StreamSubPhase::Encode` (`egress_flush.rs:116-119`) times
      only `flush_buffer`, NOT `encode_do_get`'s IPC framing. Add a sub-phase around the encoder
      stream, or record explicitly that levers 4/5 are attributed by `perf` alone.
- [x] **Re-baseline pre-change** on the regenerated corpus (measured; artifacts committed here,
      the rig that produced them re-anchored to #3272): bare scan and `do_get` (bypass arm)
      rows/s + cycles/row, warm and cold. This pair, not 210,192 / 312,155, is the baseline.

## 1. File-size precursor (surface: `cqlite-core/src/export/arrow_convert.rs`)
- [x] Split `arrow_convert.rs` (2,596 lines, ~4x the ~800 target) by responsibility before any
      behavioral lever — builders / schema / conversion entry — so the `file-size` ratchet never
      forces `CQLITE_ALLOW_FILE_GROWTH=1`. Mechanical, no behavior change; digest unchanged.
- [x] Check `cqlite-flight/src/producer.rs` (3,243 lines) for the same problem before touching it.
      → checked; it **shrank to 3,230** because `flush_buffer` moved to `egress_flush.rs` beside its
      only caller, so the `file-size` ratchet is satisfied without `CQLITE_ALLOW_FILE_GROWTH=1`.
      **That move SURVIVES the lever-6 revert** — its location was never part of the lever, and the
      ratchet forbids regrowing an over-threshold file, so moving it back is not an option. A
      responsibility split of `producer.rs` itself remains owed to epic #1116 and is out of scope.

## 2. Lever 4 + 6 — the cheap floor (surfaces: `cqlite-flight/src/batch_bytes.rs`, `streaming.rs`, `cqlite-core/src/export/arrow_convert.rs`)
- [ ] **REVERTED — deferred to #3281 (owner ruling A, 2026-08-03).** Lever 4: align
      `DEFAULT_MAX_BATCH_BYTES` (`batch_bytes.rs:154`, 4 MiB) with arrow-flight's
      `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES` (2 MiB, `encode.rs:166`) — or raise the encoder's limit —
      so a batch is not re-sliced and framed twice. Re-derive the narrow-shape table at
      `batch_bytes.rs:137-153`; halving the cap moves where the byte-cap starts binding.
      It WAS implemented (the encoder's limit raised, not the batch cap lowered) and it
      **measured at ZERO** (−0.03%); the wire-safety mechanism that then justified keeping it
      conflated a `target` with a `ceiling`, so the whole surface is removed. `batch_bytes.rs`
      is byte-identical to `origin/main` again. Unchecked is the honest state: **measured at
      zero AND not retained.**
- [ ] **REVERTED (owner ruling, 2026-08-04).** Lever 6: cache the Arrow `Schema` instead of
      rebuilding it per batch at `arrow_convert.rs:201-203`. It WAS implemented (`EgressBatchPlan`,
      one `Schema` per merge) and it **measured at ZERO TWICE** — first at −0.2% while a redundant
      per-batch validation defeated the mechanism (so that reading was lever 6 **plus a bug**, not a
      result about the lever), then at **+0.30% with a 95% CI covering zero, 4.5x below the ~1.4%
      between-binary layout noise floor**, with the mechanism genuinely live. The egress path builds
      the schema per batch again; `producer_drive.rs`, `producer_stream.rs` and the `flush_credited`
      signature are back to `origin/main`. Unchecked is the honest state: **measured at zero AND not
      retained.** See the LEVER 6 REVERTED block at the top of this file.
- [x] Measure each individually against the Phase-0 baseline; report rows/s AND cycles/row.
- [x] `--lite` + the digest oracle + `issue_2825_max_batch_bytes_e2e.rs` after each.

## 3. Lever 2 + 3 — builder and estimator passes (surfaces: `arrow_convert.rs`, `arrow_size.rs`, `arrow_columnar.rs`)

> **NOT IMPLEMENTED, deliberately (owner ruling 2026-08-03).** The kill criterion
> (spec R5) fired: nothing measured prices either lever, because the region they
> live in is an unattributed **complement** (§"DELIVERY STATUS"), and stacking a
> lever on an unattributed bucket is exactly what R5 forbids. **Re-anchored to
> #3248**, which buys the per-function profile and the shared-vs-Flight-marginal
> differential FIRST. Leaving these unchecked is the honest state, not an omission.

- [ ] Lever 2: replace the intermediate `Vec<Option<T>>` in each scalar builder
      (`arrow_convert.rs:1407,1461,1487,1524,1555,1596`) with `PrimitiveBuilder::with_capacity(n_rows)`
      appends + an explicit null bitmap. Note `:1555`/`:1596` hold BORROWED `&str`/`&[u8]` — pointer
      vectors, not data copies — so their gain is smaller; measure, do not assume.
- [ ] Lever 3: fold `estimate_arrow_row_bytes` (`arrow_size.rs:251`, per-cell map probe at `:254`)
      into the transpose/append pass, removing the third hash pass per row.
- [ ] Prove the invariant survives: `arrow_size_tests.rs` (`Σ estimate >= realized payload` over
      `arrow_shape_corpus.rs`) and `batch_bytes_tests::the_capacity_bound_holds_over_the_shared_shape_corpus`
      both green, and the byte-cap cut at `producer_stream.rs:351-358` cuts on the same rows.
- [ ] Measure each individually; digest unchanged.

## 4. Lever 1 — column-major build from the scan row carrier (surfaces: `cqlite-core/src/query/select_executor/row_build.rs`, `cqlite-flight/src/producer.rs`)

> **NOT IMPLEMENTED, deliberately (owner ruling 2026-08-03).** Routed to **#3231**
> as an unpriced candidate. #3248 records the reason this one is the riskiest to
> fund on instinct: the per-row `HashMap` is built on `cqlite-core`'s **shared**
> row path, which the AC1 bare-scan control arm also pays — so removing it could be
> the largest absolute win in the 0.17 program **and nearly worthless for AC1's
> ratio**. Only the differential profile can tell those apart.

- [ ] Add a column-major emit seam alongside `build_row_from_scan_cached` (`row_build.rs:227`) that
      writes cells positionally from the authoritative schema, skipping the per-row
      `HashMap<Arc<str>, Value>` (`:246`) and the two downstream hash probes
      (`arrow_columnar.rs:87`, `arrow_size.rs:254`).
- [ ] Column identity SHALL come from the ticket schema positionally — never inferred from bytes
      (#28). No `unwrap()`/`expect()`; clean under `RUSTFLAGS="-D warnings"`.
- [ ] Keep the `QueryRow` path intact for every non-`do_get` consumer; do not fork the adapter.
- [ ] Prove output invariance: the digest oracle, `issue_3058_forced_path_differential.rs`,
      `query_semantics_flight_parity.rs` + core `query_semantics_oracle_parity.rs` at pinned `now`,
      and `do_get_transport_test.rs`'s wire-frame golden.
- [ ] Measure individually. If lever 1 does not move rows/s beyond the spread, STOP — see §6.

## 5. Lever 5 + 7 — optional, gated (surfaces: `cqlite-flight/src/streaming.rs`, `row_build.rs`)

> **NOT IMPLEMENTED — the recorded decision (owner ruling 2026-08-03).** Both were
> declared optional by this section's own framing, and lever 7 "may be dropped
> without failing the change". Lever 5 changes wire behavior for a framing gain the
> kill criterion no longer justifies; lever 7 is explicitly gated behind lever 1,
> which is not implemented. **Decision: dropped for this change**, recorded here as
> required rather than left silent.

- [ ] Lever 5 (only per the Seam-1 decision): `DictionaryHandling::Resend` or hydrate-once at
      `streaming.rs:599`, since the schema has no dictionary columns and `hydrate_dictionaries`
      (`encode.rs:685`) is a pure per-batch `RecordBatch` rebuild. Verify against a real Trino/JDBC
      client shape, not only `FlightRecordBatchStream`; the byte golden is the guard.
- [ ] Lever 7 (stretch, strictly behind lever 1): borrow text/blob from the decoded chunk instead of
      `into_owned()` (`row_build.rs:259`, `types.rs:1234`). Requires a chunk-retention story that
      holds the <128MB bound. **May be dropped without failing the change** — record the decision.

## 6. Acceptance measurement + the kill criterion (correctness must be green FIRST)
- [ ] Final same-session run: bare scan and `do_get`, same box, same pinned sibling pair, same
      bytes, median of 3, warm and cold separately, rows/s AND cycles/row, spread stated.
      → **WARM: done and exceeded** (10 interleaved rounds per arm, not 3 reps; every per-run
      number committed in `abc-interleaved-runs.json`). **COLD: NOT re-measured post-lever** — it
      exists only at Phase-0 (`baseline-2026-08-03.md`), where both arms are page-in bound and the
      1.3x is met trivially and uninformatively. **Left unchecked because this item is not fully
      satisfied**; the warm figure is this issue's owned claim, and no cold claim is made for the
      post-lever binaries. Cold re-measurement carries to #3248.
- [x] PASS = `do_get rows/s >= bare_scan rows/s / 1.3` in that same session. Never a CPU-share
      claim; a fall in "% cycles in Arrow encode" with unmoved rows/s is a **FAIL** (#2877 shape).
      → the criterion was applied **exactly as specified and it returned NOT MET**. Two sessions, both
      NOT MET, neither comparable to the other (no absolute on this box is reusable cross-session):
      at the **SUPERSEDED 4 MiB target**, 217,791 vs a 256,131 target (15.0% short); at the **SHIPPED
      3.875 MiB target**, ratio 1.553x — 230,321 vs a 275,223 target (16.3% short, −44,902 rows/s),
      which is the delivered figure. Checked = "applied as specified", **NOT** "PASS achieved". No
      CPU-share claim is made anywhere.
- [x] Report each lever's individual delta, including any lever that cost throughput.
- [x] **Kill criterion:** if the levers do not move rows/s materially, STOP — post the negative
      result (rows/s + cycles/row, warm and cold), do not stack further levers, re-open attribution.
      A correctly measured, correctly reported negative result CLOSES this change satisfactorily.
- [x] State plainly what remains OWED: the WS0 absolutes, the stock-Cassandra leg (no Cassandra on
      this box), and the compressed-corpus shape (#1406). Never restate them as reproduced.

## 7. Scope-fence check before review
- [x] No allocator work (#3028) and no `RowColumnResolution` hoist (#3047) smuggled in — flag them
      for re-pricing instead.
- [x] No mpsc handoff-limiter change; no cold-route (#3068) change.
- [x] No query-result change: digest, differential, wire golden, semantics oracles all unchanged.
- [x] No new public CLI/Python/Node surface; Flight wire format unchanged (or lever 5's change is
      explicitly owner-approved and byte-pinned).

## 8. Docs / doctrine
- [x] Commit the method doc (traps, pinning, denominators, warm/cold separation) with the scripts.
- [x] Re-price `docs/architecture/throughput-program-2026-07.md` and the #3028 / #3047 estimates
      against the post-change profile — or record that the kill criterion fired and they stand.
      → **the second branch is taken: the kill criterion fired and they stand.** That doc carries no
      #3096-specific estimate to re-price (its only Arrow-encode figure is a 1.0% line for a
      two-column shape, a different context), and re-pricing #3028 / #3047 against an unattributed
      **complement** would be exactly the instinct-funding #3248 exists to stop.
- [x] Update `CLAUDE.md` / the website `agents-developing/` page only if a user-facing or workflow
      rule actually changed; verify a publish by grepping the served page for new content, never by
      HTTP 200.

## 9. Gate + review + sign-off
- [x] `--lite` every fix round (summary-file redirect); never a full gate per round.
- [ ] `rust-reviewer` + `bash scripts/flow/roborev-review.sh --agent <agent> --model <model>
      --repo /home/ubuntu/projects/cqlite-wt/issue-3096` on the lite-green diff, BEFORE the full gate.
- [ ] Open the PR; hand the endgame to `flow-closer`: ONE full `scripts/agent-gate.sh` → `C`
      (spec-auditor against `openspec/changes/arrow-encode-doget/specs/**`) → final roborev →
      `gh pr merge --auto --squash --delete-branch` on green → `flow-finalize`.
- [ ] Paste the full `AGENT-GATE SUMMARY` (verify `RESULT: PASS|FAIL` and `tree-integrity:`) and the
      `ROBOREV REVIEW SUMMARY` in the PR.
