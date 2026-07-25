# Tasks: reconcile-overlap-multiplier (issue #2043 / M9)

## 1. Fixture

- [ ] 1.1 Add a k-parameterized multi-generation builder to `cqlite-core/benches/fixtures/mod.rs`,
      generalizing the same-`(pk, ck)`-across-generations pattern of
      `benches/compaction.rs::build_tombstone_heavy` (:307). Signature takes `k` and a collision-mix
      selector; built on `open_write_engine()` (:340) + `seeded_rng()` (:40).
      **Surface exercised:** `WriteEngine` flush → k `Data.db` files.
- [ ] 1.2 Assert in-builder that exactly `k` `Data.db` files exist before returning (mirrors
      `tests/issue_1579_streaming_multigen_order.rs::count_data_files` :88).

## 2. Benchmark

- [ ] 2.1 Add `cqlite-core/benches/reconcile_overlap.rs` + its `[[bench]]` declaration in
      `cqlite-core/Cargo.toml` (`required-features = ["write-support"]`, matching `compaction`).
- [ ] 2.2 Drive the merge through `KWayMerger::new_from_readers` (`merge/from_readers.rs:302`) with
      `with_now_secs` (`merge/mod.rs:2622`). **Surface exercised:** public `KWayMerger` drain.
      No new `pub` item on the reconcile path.
- [ ] 2.3 Emit `Throughput::Elements` over merged output rows; capture collisions-per-row and
      `PurgeCounts` per arm.
- [ ] 2.4 Matrix: k ∈ {1, 2, 5, 10, 20} × {`disjoint`, `lww_overwrite`, `tombstone`, `ttl_expiring`,
      `field_blend`}. `ttl_expiring` pins `now`; no-TTL arms pass `None`.
- [ ] 2.5 Assert a positive output row count per arm (never a 0-row vacuous pass).
- [ ] 2.6 Register the bench IDs under `advisory_benches` in `benches/perf-gate.json`; add no
      `threshold_pct` entry.
- [ ] 2.7 Document the one-line run command in `cqlite-core/benches/README.md`.

## 3. Measurement run (BLOCKED until the machine is quiesced)

- [ ] 3.1 Confirm no concurrent full gate and a 1-minute load average under the ceiling; record it.
- [ ] 3.2 Run the full matrix; capture machine specs + commit SHA.
- [ ] 3.3 Validate the `disjoint` k=1 anchor against the published ~2.0 µs/row singleton figure
      (`docs/research/phase2-verify-stage2.md:226-232`). Out of band ⇒ void the run, fix the harness,
      re-run. Do not derive a multiplier from a void run.
- [ ] 3.4 Re-run `ttl_expiring` at a later wall-clock time to prove expiry determinism.

## 4. Record + doc updates

- [ ] 4.1 Write `docs/research/issue-2043-reconcile-overlap-multiplier.md`: k→ns/row table, per-mix
      `cost(k)/cost(1)`, machine specs, commit SHA, load average.
- [ ] 4.2 Verdict: tightened §3 gen-overlap band (or reasoned refusal), with the assumed field k
      labeled assumption-not-measurement and #2818 cited as its replacement.
- [ ] 4.3 Verdict: conditional L3 disposition — the k-band making ~1.20× correct vs the band making
      ~1.03–1.08× correct, with arithmetic; note the final call follows field k.
- [ ] 4.4 Update `docs/architecture/throughput-program-2026-07.md` §3 (gen-overlap term, :127-129) and
      the §4 L3 tension flag (:168-175) to cite the record.
- [ ] 4.5 Post the L3 finding to #2822 (M7) — it is blocked on this data.

## 5. Quality stages

- [ ] 5.1 `--lite` green each fix round (summary-file redirect).
- [ ] 5.2 `rust-reviewer` + roborev on the lite-green diff, BEFORE the full gate (review-first).
- [ ] 5.3 Open the PR; hand the endgame to `flow-closer`.
- [ ] 5.4 ONE full `scripts/agent-gate.sh` of record — serialized against any other gate on this box.
      Per the #2751 workaround, run WITHOUT `AGENT_GATE_SUMMARY_FILE` and read
      `<worktree>/.agent-gate-summary.txt`.
- [ ] 5.5 `spec-auditor` (C) anchored to `openspec/changes/reconcile-overlap-multiplier/specs/**`.
- [ ] 5.6 Final roborev pass → merge-on-green → `flow-finalize` (archive, telemetry stamp via
      telemetry PR, worktree + branch removal, issue close).
