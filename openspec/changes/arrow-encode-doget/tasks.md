# Tasks: arrow-encode-doget (issue #3096)

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

**So BOTH landed levers now measure at zero at the shipped target.** Lever 4 is
retained for **wire safety** (every `data_body` under the reserved ceiling, at a
capacity/payload ratio of ~1.0 — asserted by
`cqlite-flight/src/streaming_framing_tests.rs`), lever 6 for being strictly less
work per batch. Neither is retained on a throughput claim.

**Spec R5 (owner-approved) makes a correctly-measured, correctly-reported
negative result a satisfying outcome of THIS change. It does not make AC1
satisfied, and nothing in this tree may say otherwise.** We do not launder a
negative result through an optimistic title. The **C** intent audit should record
**AC1: unmet**; the spec has deliberately NOT been edited to soften it.

**What the change delivers instead, all of it verifiable:** the committed
reproduction rig (`tools/ws0-corpus-gen` + `scripts/perf/`), the in-repo
Arrow-buffer digest oracle, the closed IPC-framing attribution blind spot
(**313.0 ns/row**, previously attributable to nothing), lever 4 (a **wire-safety**
lever: bodies provably under the reserved gRPC ceiling; **throughput measured at
zero** at the shipped target), lever 6 (**measured at zero**, recorded as such),
the cross-session drift finding — observed TWICE now — and the honest 16.3% gap
with its per-run evidence.

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
- [x] **Corpus generator** (surface: new `tools/ws0-corpus-gen` binary): drives the production
      `cqlite_core` `SSTableWriter` from the pinned `ws0.events` DDL
      (`docs/reports/ws0-3100-artifacts/ws0-h2h/schemas/ws0-events.cql`); 4,000,000 rows as
      40,000 partitions x 100 rows; partitions emitted in Murmur3 token order; **uncompressed**
      (assert no `CompressionInfo.db` is written — #1406); deterministic from a recorded seed.
- [x] Generator self-checks: re-running with the same seed produces a byte-identical `Data.db`;
      writing/observing 0 rows exits non-zero; the emitted row/partition counts are asserted, not
      assumed.
- [x] Record the corpus identity in-tree: `sha256`, row count, cells/row, on-disk bytes, bytes/row.
      State explicitly that it differs from #3058's `0185909de6da…` by construction and that the
      old digest is NOT asserted.
- [x] Document the corpus as a **PERFORMANCE FIXTURE ONLY** in the generator's own docs — a
      CQLite-written + CQLite-read corpus is invariant to a uniform framing error (#3042) and is
      never a correctness oracle for on-disk framing.
- [x] **Measurement scripts** (surface: `scripts/perf/…` + method doc under `docs/reports/`):
      CPU-wide `perf stat -C <cpu-list>`, **no** `perf stat -p` anywhere; `taskset` pinning to a
      pair read from `/sys/devices/system/cpu/cpu*/topology/thread_siblings_list` that **fails
      closed** if the pair is not siblings of one physical core; median of 3 with spread; setup
      subtracted from the cycles/row denominator; row denominator printed with every figure.
- [x] Both arms in ONE session on ONE pinned pair: bare scan via `execute_streaming`, Flight
      `do_get` over a real loopback transport. Warm and cold as separate runs, reported separately.
- [x] **In-repo Arrow-buffer digest oracle** (surface: `cqlite-flight/tests/`): folds each emitted
      `RecordBatch`'s value + validity buffers in column order plus row count; asserts equality
      across `CQLITE_FLIGHT_MERGE_PATH=bypass|merge` at a PINNED `now`, plus row count and
      cells/row. Shares `PROBE_LOCK` with `issue_3058_forced_path_differential.rs` (process-global
      env). One `#[test]`, cases in a list.
- [x] Close the attribution blind spot: `StreamSubPhase::Encode` (`egress_flush.rs:116-119`) times
      only `flush_buffer`, NOT `encode_do_get`'s IPC framing. Add a sub-phase around the encoder
      stream, or record explicitly that levers 4/5 are attributed by `perf` alone.
- [x] **Re-baseline pre-change** on the regenerated corpus: bare scan and `do_get` (bypass arm)
      rows/s + cycles/row, warm and cold. This pair, not 210,192 / 312,155, is the baseline.

## 1. File-size precursor (surface: `cqlite-core/src/export/arrow_convert.rs`)
- [x] Split `arrow_convert.rs` (2,596 lines, ~4x the ~800 target) by responsibility before any
      behavioral lever — builders / schema / conversion entry — so the `file-size` ratchet never
      forces `CQLITE_ALLOW_FILE_GROWTH=1`. Mechanical, no behavior change; digest unchanged.
- [x] Check `cqlite-flight/src/producer.rs` (3,243 lines) for the same problem before touching it.
      → checked; lever 6 **shrank** it (schema build hoisted out to `egress_flush.rs`), so the
      `file-size` ratchet is satisfied without `CQLITE_ALLOW_FILE_GROWTH=1`. A responsibility split
      of `producer.rs` itself remains owed to epic #1116 and is out of this change's scope.

## 2. Lever 4 + 6 — the cheap floor (surfaces: `cqlite-flight/src/batch_bytes.rs`, `streaming.rs`, `cqlite-core/src/export/arrow_convert.rs`)
- [x] Lever 4: align `DEFAULT_MAX_BATCH_BYTES` (`batch_bytes.rs:154`, 4 MiB) with arrow-flight's
      `GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES` (2 MiB, `encode.rs:166`) — or raise the encoder's limit —
      so a batch is not re-sliced and framed twice. Re-derive the narrow-shape table at
      `batch_bytes.rs:137-153`; halving the cap moves where the byte-cap starts binding.
- [x] Lever 6: cache the Arrow `Schema` instead of rebuilding it per batch at
      `arrow_convert.rs:201-203`.
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
      → the criterion was applied **exactly as specified and it returned NOT MET** (217,791 vs a
      256,131 target, 15.0% short). Checked = "applied as specified", **NOT** "PASS achieved". No
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
