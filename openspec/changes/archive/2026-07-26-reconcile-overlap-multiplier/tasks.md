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
      `cqlite-core/Cargo.toml`. As-planned wording said `required-features = ["write-support"]`; what
      landed instead **matches `compaction`** as intended — a bare `[[bench]]` (`harness = false`, no
      `required-features`) plus an in-file `#[cfg(not(feature = "write-support"))]` no-op `main`, the
      same pattern as `benches/compaction.rs:456`. Equivalent gating, and it keeps the target compiling
      (as a no-op) under `--no-default-features` instead of vanishing from the build graph.
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

## 3. Measurement run (COMPLETE — 2 valid runs, re-measured after the review-round fixture fixes AND the owner-decided setup-bias re-measure; run-start load1m 0.14 / 0.51, peak per-interval FOREIGN CPU 0.946 of 16 cores, 27/27 arms gated per run — matches record §1)

- [x] 3.1 Confirm no concurrent full gate and a 1-minute load average under the ceiling; record it.
- [x] 3.2 Run the full matrix; capture machine specs + commit SHA.
- [x] 3.3 Validate the anchor against the published ~2.0 µs/row singleton figure
      (`docs/research/phase2-verify-stage2.md:226-232`). Out of band ⇒ void the run, fix the harness,
      re-run. Do not derive a multiplier from a void run.
      **Result:** the anchor (amended by owner decision to the SATURATED `disjoint` control, mean of
      k ≥ 5) lands at **2.81 µs/row = 1.40×** the published figure, inside the stated ±50 % band. The
      k=1 point is 2.72× the published figure and 1.937× the anchor, and that deviation is now
      DECOMPOSED by measured arms with no residual: 1.118× scan width × 1.429× (1→2 producers,
      `producer_control/{p1,p2}`) × 1.212× (2→5+ producers) = 1.937×, against Phase-0's own 1.79×
      producer/coordinator prediction for the producer-only part (measured 1.733×). Both the
      spec-mandated `cost(k)/cost(1)` and the pipeline-matched derate are published — record §3.
- [x] 3.4 Re-run `ttl_expiring` at a later wall-clock time to prove expiry determinism.
- [x] 3.5 **Review round 2 (roborev blockers) — fixture fixed and EVERYTHING re-measured.** The
      `tombstone` arm's row tombstone was reconciled away at FLUSH time (leaving a cell-less row
      tombstone, so the live-vs-row-tombstone collision never reached the merge); `field_blend`'s
      tombstone kind depended on `k`, confounding depth with composition. Both fixed (row tombstones
      now stamped BELOW their generation's live cells; `generation_mutations` takes no `k`), every arm
      now asserts a full collision-shape census + cross-k composition invariance, and the whole matrix
      was re-run twice. All published numbers, fits, `D(o)`, the §6 L3 table and
      the verdicts are recomputed from the NEW runs. Verdicts unchanged; finding "a shadowed row is
      cheaper" WITHDRAWN as a fixture artifact (deletion collisions cost more than plain overwrite).
- [x] 3.6 **Review round 4 (roborev + owner decision 2026-07-26) — validity guard made genuinely
      fail-closed and EVERYTHING re-measured with per-drain setup amortized.** (a) The tier-2 probe
      returned `None` on an unreadable `/proc`, printed `foreign_cpu_cores=unavailable`, and published
      the arm's number as if validated — it now PANICS on an unreadable probe, gates the per-interval
      MAXIMUM (`iter_custom` per sample batch) instead of a whole-arm mean, derives `cores` from the
      same `/proc/stat` the busy figure comes from, extracts own-ticks through a self-tested pure
      `own_ticks_from`, and the run asserts `arms_gated == arms`. The interval-length floor is DERIVED
      (one stray tick ≤10 % of the ceiling ⇒ 160 ticks) and `SamplingMode::Flat` is pinned so no
      published sample is ever too short to gate. (b) The timed region contains `new_from_readers`
      (k thread spawns + k adapter opens), which at ~1024 rows/arm biased `cost(k)/cost(1)` upward
      with k; the arm width was quadrupled on the PARTITION count (16 → 64, leaving the
      `MergeStep::Partition` batch width invariant — the rejected `ck` = 256 variant moved the anchor
      +7 %) and every arm now MEASURES and prints its setup share (0.20–0.24 % at k = 1, 0.37–0.85 %
      at k = 20 ⇒ ≤0.6 % effect on any multiplier). Whole matrix re-run twice at commit `562f14aa`;
      p/q, `D(o)`, the band⇔`o` mapping and the §6 L3 table recomputed. **No verdict changed.**

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
- [x] 5.4 ONE full `scripts/agent-gate.sh` of record — **`RESULT: PASS`, all components PASS, zero
      FAILs**, `dirty: no`, `datasets: 144 Data.db files under /data/datasets`,
      `cores-per-gate=16`. An earlier full PASS at `c7a2af4c` was **invalidated** when the instrument
      was rebuilt for the setup-bias re-measure; the gate of record is the LATER run, taken after the
      final rebase onto `origin/main` (its `commit:` line names the certified SHA — a commit cannot
      cite its own hash, so it is not repeated here). **This docs-only task/record correction was
      re-certified by `scripts/agent-gate.sh --delta` against that full PASS**, which fails closed on
      any non-docs/test diff. The `#2751` workaround noted as-planned was NOT needed:
      `AGENT_GATE_SUMMARY_FILE` worked as documented. Both verbatim blocks posted on PR #2892.
- [x] 5.5 `spec-auditor` (C) anchored to `openspec/changes/reconcile-overlap-multiplier/specs/**`.
      An earlier C PASS was **voided** along with its run (instrument rebuilt, every published number
      changed), so C was **re-run against the new numbers**. Re-run verdict: **7/8 requirements
      `satisfied`; Requirement 6 `partial`** on one blocking docs-only finding (F1) — the record's
      run-metadata row claimed the measurement commit was a "reachable ancestor of the branch head",
      false after the rebase and unactionable after a squash merge. Fixed in this commit: the row now
      states the non-ancestry plainly, proves byte-identity of all seven instrument blobs, and gives
      the `refs/measurements/issue-2043-run-562f14aa` preservation ref + fetch command so a reader can
      actually obtain the measured instrument. The auditor independently re-derived **every** published
      figure from the §2 table (all 20 `cost(k)/cost(1)` and 20 `D` cells, p=1689/q=1127 ⇒ the band
      ⇔ o ∈ [1.17, 1.83], the f/S(o) tables, the o=2 drain, the out-of-sample `field_blend` deltas) and
      confirmed no stale pre-re-measure figure survives as live. The owner-approved (2026-07-26)
      saturated-anchor amendment was verified consistent across spec, design, README and this file,
      with the general void rule intact for the saturated control.
- [x] 5.6 Final roborev pass → merge-on-green → `flow-finalize`. **Final roborev: 4 findings (1
      Medium, 3 Low), all in the advisory-only bench instrument, none altering a published figure.**
      The Medium's *documentation* half was fixed pre-merge (record §1 now declares the instrument's
      **≥7-core host floor** — a published 250 ms `Flat` sample advances `25 × cores` ticks, so the
      160-tick resolution floor needs `cores ≥ 6.4` — and records that the two small-host ceiling
      floors are consequently unreachable dead config). All four **src** fixes were batched into
      **#2930** (up-front core-count assert + dead-floor cleanup; fail-closed `/proc/stat` parse in
      `cpu_ticks`, which today `filter_map`s and could index-shift fail-OPEN; `perf-gate.json` arm-id
      drift check; executing coverage for `foreign_cores`/`end_arm`). Deferring them was a deliberate,
      owner-backed **provenance** decision, not a scheduling one: editing the guard AFTER the
      measurement would falsify this record's byte-identity guarantee between the merged instrument
      and the preserved measurement commit `refs/measurements/issue-2043-run-562f14aa` — the exact
      claim C blocked on (F1) — in exchange for changes that cannot move a number (the parse defect is
      unreachable on Linux; the host floor never binds at 16 cores). #2930 lands with #2898, which
      re-runs the census anyway. **PR #2892 squash-merged 2026-07-26T19:10:50Z** via armed `--auto` on
      the `required` lane going green; `premerge-assert.sh` returned `PREMERGE: OK 85fecae5`.
      Telemetry stamped and this change archived in the same follow-up PR.
