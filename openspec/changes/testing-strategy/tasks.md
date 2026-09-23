# Tasks: testing-strategy (issue #4259)

Each phase ships as its own child issue / PR (1:1:1:1). Every PR follows the implement loop:
`--lite` per round → rust-reviewer + roborev
(`bash scripts/flow/roborev-review.sh --agent <agent> --model <model>`) → flow-closer
{ rebase → full gate once → C (spec-auditor vs this spec) → roborev last → premerge-assert → arm }.

## 1. Doctrine + public docs (#4258)

- [ ] 1.1 Write `website/src/content/docs/agents-developing/testing-doctrine.md` (~400 lines): oracle
      classes + blind spots (moved from `CLAUDE.md`), layers, anchor reasons, differential contract,
      tiers, budgets. Surface: docs site.
- [ ] 1.2 Slim `CLAUDE.md` "Testing" to hard rules + link. Surface: agent context.
- [ ] 1.3 Public section `website/src/content/docs/testing/` (landing, oracles, differential, guards,
      tiers/fuzzing, known blind spots linking #4255/#4256/#4257); sidebar entry after User Docs;
      homepage Key Features card + link card; Field Validation cross-link. Surface: site build.
- [ ] 1.4 Archive stale `docs/testing/*` CLI-era docs to `docs/archive/`; retitle `docs/TESTING_PRD.md`.
- [ ] 1.5 Add anchor-vs-matrix and oracle-class checks to `rust-reviewer` and the roborev-findings page.
- [ ] 1.6 `npm run build` in `website/` clean; every factual claim verified at PR head.

## 2. Differential contract (#4255, then #4256)

- [ ] 2.1 `cqlite-core/tests/support/differential.rs`: `ExpectedOutcome`, `DifferentialCase`, runner
      returning `Result`, engagement check via `access_path`, cold-leg invalidation via
      `GlobalKeyOffsetCache::global().invalidate_all()`. Surface: point-vs-full lane.
- [ ] 2.2 Confirm with evidence whether the discovery scan warms `GlobalKeyOffsetCache`; add a test that
      the point leg's first probe is a MISS.
- [ ] 2.3 Port `point_vs_full_differential.rs` onto the runner; every existing case declares `Served`.
- [ ] 2.4 Debug-only fault seam on the point read path (`cfg(debug_assertions)`), meta-test asserting
      the runner returns a divergence; release-build inertness test.
- [ ] 2.5 (#4256) Add `SupportedUnserved` and `GatedFallback` cases; fault meta-test for the
      query-semantics oracle suite.

## 3. Matrix support + first matrix (#4256)

- [ ] 3.1 `tests/support/matrix.rs`: deterministic 2-wise builder, pruning predicate, full-product
      pairs, case-floor assertion; unit tests proving pair coverage + determinism.
- [ ] 3.2 `tests/support/fixture_cache.rs`: content-addressed cache under `CARGO_TARGET_TMPDIR`, atomic
      publish, generator-version key; concurrency test with two processes; cache-hit test.
- [ ] 3.3 Reverse × format × compression rows in the point-vs-full matrix, incl. compressed BTI;
      module header names full-product pairs.
- [ ] 3.4 Delete or wire `tests/src/bin/property_based_test_runner.rs`.

## 4. Tiers (new child issue)

- [ ] 4.1 `.config/nextest.toml`: `pr` / `nightly` / `weekly` profiles; `burn_*` excluded from `pr`.
- [ ] 4.2 Randomized harness refuses fresh seeds outside `nightly`/`weekly`; fixed-seed array replayed
      in `pr`.
- [ ] 4.3 Attach nightly burn to an existing scheduled workflow; weekly Cassandra replay to the
      existing Docker lane; failure → issue with seed + repro, never a merge block.

## 5. Budgets (new child issue)

- [ ] 5.1 Measure per-binary nextest wall time on the gate box (3 runs); commit
      `scripts/ci/test-budgets.txt` at baseline + 15%.
- [ ] 5.2 Gate component emits `test-budget:` SUMMARY line from nextest JUnit; FAIL on breach naming
      binary, time, budget, remedies; self-test in `scripts/tests/`.
- [ ] 5.3 Document the component in `docs/development/gate-ops.md`.

## 6. Consolidation measurement (new child issue)

- [ ] 6.1 On a branch, merge ≥20 `issue_*` targets into `tests/regressions/main.rs` + modules.
- [ ] 6.2 Record cold/warm build, link time, `target/` size, nextest wall — before/after, gate box.
- [ ] 6.3 Report in the issue; open a migration issue only if ≥10% `core-tests` wall or ≥20% deps
      size; record the result in the doctrine page either way.

## 7. Randomized generator (#4257 — separate OpenSpec change)

- [ ] 7.1 Activate #4257 as its own design-driven change satisfying the randomized-testing contract
      requirement above.

## 8. Close-out

- [ ] 8.1 C (spec-auditor) per child PR against the requirements it claims.
- [ ] 8.2 Archive this change when phases 1–6 are merged (7 tracks separately).
