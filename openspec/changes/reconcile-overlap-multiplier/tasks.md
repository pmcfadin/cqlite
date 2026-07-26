# Tasks: reconcile-overlap-multiplier (issue #2043 / M9)

## 1. Fixture

- [x] 1.1 Add a k-parameterized multi-generation builder to `cqlite-core/benches/fixtures/multigen.rs`
      (a new sibling module re-exported from `fixtures/mod.rs`; as-planned wording said `mod.rs`, but it
      landed in its own file to keep `mod.rs` under the file-size ratchet),
      generalizing the same-`(pk, ck)`-across-generations pattern of
      `benches/compaction.rs::build_tombstone_heavy` (:307). Signature takes `k` and a collision-mix
      selector; built on `open_write_engine()` (:340) + `seeded_rng()` (:40).
      **Surface exercised:** `WriteEngine` flush → k `Data.db` files.
- [x] 1.2 Assert in-builder that exactly `k` `Data.db` files exist before returning (mirrors
      `tests/issue_1579_streaming_multigen_order.rs::count_data_files` :88).

## 2. Benchmark

- [x] 2.1 Add `cqlite-core/benches/reconcile_overlap.rs` + its `[[bench]]` declaration in
      `cqlite-core/Cargo.toml` (`required-features = ["write-support"]`, matching `compaction`).
- [x] 2.2 Drive the merge through `KWayMerger::new_from_readers` (`merge/from_readers.rs:302`) with
      `with_now_secs` (`merge/mod.rs:2622`). **Surface exercised:** public `KWayMerger` drain.
      No new `pub` item on the reconcile path.
- [x] 2.3 Emit `Throughput::Elements` over merged output rows; capture collisions-per-row per arm.
      **`PurgeCounts` was NOT captured, by design:** it is a private struct in `merge/mod.rs` (:2388)
      with no public accessor, so reading it from a bench would have required adding a `pub` surface
      purely for instrumentation. The record instead documents `purges=0(read-merge)` as a **justified
      structural zero** — the read-merge path performs no purges by mechanism, not by measurement — so
      the quantity this task wanted is accounted for without widening the public API. As-planned
      wording ("capture `PurgeCounts`") is superseded by that disposition.
- [x] 2.4 Matrix: k ∈ {1, 2, 5, 10, 20} × {`disjoint`, `lww_overwrite`, `tombstone`, `ttl_expiring`,
      `field_blend`}. `ttl_expiring` pins `now`; no-TTL arms pass `None`.
- [x] 2.5 Assert a positive output row count per arm (never a 0-row vacuous pass).
- [x] 2.6 Register the bench IDs under `advisory_benches` in `benches/perf-gate.json`; add no
      `threshold_pct` entry.
- [x] 2.7 Document the one-line run command in `cqlite-core/benches/README.md`.

## 3. Measurement run (COMPLETE — 2 valid runs, re-measured after the review-round fixture fixes; run-start load1m 0.73 / 0.66, peak FOREIGN CPU 0.20 of 16 cores)

- [x] 3.1 Confirm no concurrent full gate and a 1-minute load average under the ceiling; record it.
- [x] 3.2 Run the full matrix; capture machine specs + commit SHA.
- [x] 3.3 Validate the anchor against the published ~2.0 µs/row singleton figure
      (`docs/research/phase2-verify-stage2.md:226-232`). Out of band ⇒ void the run, fix the harness,
      re-run. Do not derive a multiplier from a void run.
      **Result:** the anchor (amended by owner decision to the SATURATED `disjoint` control, mean of
      k ≥ 5) lands at **2.82 µs/row = 1.41×** the published figure, inside the stated ±50 % band. The
      k=1 point is 2.70× the published figure and 1.92× the anchor, and that deviation is now
      DECOMPOSED by measured arms with no residual: 1.106× scan width × 1.424× (1→2 producers,
      `producer_control/{p1,p2}`) × 1.216× (2→5+ producers) = 1.916×, against Phase-0's own 1.79×
      producer/coordinator prediction for the producer-only part (measured 1.73×). Both the
      spec-mandated `cost(k)/cost(1)` and the pipeline-matched derate are published — record §3.
- [x] 3.4 Re-run `ttl_expiring` at a later wall-clock time to prove expiry determinism.
- [x] 3.5 **Review round 2 (roborev blockers) — fixture fixed and EVERYTHING re-measured.** The
      `tombstone` arm's row tombstone was reconciled away at FLUSH time (leaving a cell-less row
      tombstone, so the live-vs-row-tombstone collision never reached the merge); `field_blend`'s
      tombstone kind depended on `k`, confounding depth with composition. Both fixed (row tombstones
      now stamped BELOW their generation's live cells; `generation_mutations` takes no `k`), every arm
      now asserts a full collision-shape census + cross-k composition invariance, and the whole matrix
      was re-run twice at commit `620eea70` (recorded pre-rebase as `6f894d67`; same tree, see the
      record's §1 commit row). All published numbers, fits, `D(o)`, the §6 L3 table and
      the verdicts are recomputed from the NEW runs. Verdicts unchanged; finding "a shadowed row is
      cheaper" WITHDRAWN as a fixture artifact (deletion collisions cost +3.0 % over plain overwrite).

## 4. Record + doc updates

- [x] 4.1 Write `docs/research/issue-2043-reconcile-overlap-multiplier.md`: k→ns/row table, per-mix
      `cost(k)/cost(1)`, machine specs, commit SHA, load average.
- [x] 4.2 Verdict: tightened §3 gen-overlap band (or reasoned refusal), with the assumed field k
      labeled assumption-not-measurement and #2818 cited as its replacement.
- [x] 4.3 Verdict: conditional L3 disposition — the k-band making ~1.20× correct vs the band making
      ~1.03–1.08× correct, with arithmetic; note the final call follows field k.
- [x] 4.4 Update `docs/architecture/throughput-program-2026-07.md` §3 (gen-overlap term, :127-129) and
      the §4 L3 tension flag (:168-175) to cite the record.
- [x] 4.5 Post the L3 finding to #2822 (M7) — it is blocked on this data.
      Posted: https://github.com/pmcfadin/cqlite/issues/2822#issuecomment-5081770565
      Corrected after the review-round re-measurement (verdict unchanged; p/q and the withdrawn
      "shadowed rows are cheaper" finding updated):
      https://github.com/pmcfadin/cqlite/issues/2822#issuecomment-5082035250

## 5. Quality stages

- [x] 5.1 `--lite` green each fix round (summary-file redirect).
- [x] 5.2 `rust-reviewer` + roborev on the lite-green diff, BEFORE the full gate (review-first).
      Two rounds; 3 blockers fixed with everything re-measured, 2 findings deferred to #2898 / #2899.
- [x] 5.3 Open the PR (#2892); hand the endgame to `flow-closer`.
- [x] 5.4 ONE full `scripts/agent-gate.sh` of record — **`RESULT: PASS`, 30/30 components**, at
      certified SHA `c7a2af4c` (post-rebase onto `origin/main` `0031fdf6`), `dirty: no`,
      `datasets: 144 Data.db files under /data/datasets`. The `#2751` workaround noted as-planned was
      NOT needed: `AGENT_GATE_SUMMARY_FILE` worked as documented. Verbatim block posted on PR #2892.
- [x] 5.5 `spec-auditor` (C) anchored to `openspec/changes/reconcile-overlap-multiplier/specs/**` —
      **C: PASS**, all 8 requirements `satisfied`; the owner-approved (2026-07-26) saturated-anchor
      amendment verified consistent across spec, design, README and this file, with the void rule
      intact for the saturated control.
- [ ] 5.6 Final roborev pass → merge-on-green → `flow-finalize` (archive, telemetry stamp via
      telemetry PR, worktree + branch removal, issue close).
