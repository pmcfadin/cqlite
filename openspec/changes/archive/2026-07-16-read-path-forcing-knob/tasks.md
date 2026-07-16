# Tasks — read-path-forcing-knob (#1918)

Each task names the public surface it exercises. Gate/C/roborev steps are the standing implement loop
(`--lite` each fix round; review-first; flow-closer runs the ONE full gate → C → roborev → merge).

## 1. Knob surface + resolution
- [x] Add `ReadPathMode { Auto, Point, Full }` and `QueryConfig.forced_read_path: Option<ReadPathMode>`
      (`config.rs`). Public surface: `QueryConfig`.
- [x] Resolve mode once via `OnceLock`: config → `CQLITE_READ_PATH` env (case-insensitive) → `Auto`;
      unrecognized value → distinct `Error`. Public surface: the resolver fn + `Error` variant.
      (`select_executor/forcing.rs`: `resolve_read_path_mode`; `Error::InvalidReadPath`.)
- [x] Unit tests: unset=Auto, env parse, config-over-env precedence, invalid-value loud error.
      Exercises: the resolver + `QueryConfig`. (`forcing::tests`, `config::tests::forced_read_path_*`.)

## 2. Single forcing gate over the classifier
- [x] Add `apply_forcing(outcome, mode) -> Result<ForcedPlan>` wrapping
      `classify_partition_lookup`'s return; add `FallbackReason::ForcedFullScan` and
      `Error::ForcedReadPathUnavailable { forced, reason }`. Public surface: `AccessPath`/`FallbackReason`
      (closed-enum contract), `Error`.
- [x] Wire the wrapper at all five call sites (`execute.rs` metadata/schemaless/materializing,
      `streaming.rs`, `stream_agg.rs`) — one helper (`apply_forcing` + `point_requires_engaged` +
      `point_forbids_fallback`), no per-site policy. Exercises: `Database::execute` (materializing +
      streaming SELECT surfaces).
- [x] `full` records `FallbackFullScan{ForcedFullScan}`; `point` fails closed on `Fallback` up front and
      on `engaged == false` post-call. Exercises: `AccessPath::last()` probe.
- [x] Tests: `full` on a targeted query records forced fallback + rows == `auto`; `point` on partial-pk
      errors distinctly; `point` on full-pk stays targeted. Exercises: `Database::execute` + probe.
      (`cqlite-core/tests/read_path_forcing_e2e.rs`.)

## 3. Differential-equality lane
- [x] New integration target `cqlite-core/tests/point_vs_full_differential.rs`: run the eligible corpus
      query matrix under forced `point` and forced `full`, assert normalized (rows/values/order)
      equality; pin `now` via `CQLITE_TTL_NOW_OVERRIDE_SECS`; fail-closed under `CQLITE_REQUIRE_FIXTURES=1`,
      SKIP-loud when the corpus is absent. Exercises: `Database::execute` under both forced modes.
- [x] Include multi-generation (`test_tomb` 2-gen tables), tombstone, and TTL fixtures.
- [x] Verify the lane by seeding a divergence locally (break one path, confirm red, revert) —
      done: seeded a `__seed.pop()` in the point path, lane went red naming the diverging query, reverted.
      Harness-level regression also pinned by `comparison_detects_a_seeded_divergence`.
- [x] Wire into the existing `integration-tests` gate tier (design.md placement): the target is an
      ordinary `cqlite-core/tests/*.rs` integration test picked up by the tier; OWNER-FORK (dedicated
      SUMMARY component) NOT taken.

## 4. Observability + docs
- [~] CLI `--explain` forced marker: SPLIT to a follow-up (OWNER-FORK #3). `--explain` is plan-only
      (`database.explain` never executes the query, so no `AccessPath` is produced); surfacing the
      forced/actual path there would require executing or new planner plumbing — not cheap.
- [x] Document `CQLITE_READ_PATH` in CLI/user docs as a test/debug-only control (values, `point`
      fail-closed behavior, not a perf recommendation). (`website/.../cli-reference.md`.)
- [x] Update CLAUDE.md two-oracle note + `agents-developing/` validation-playbook to name the
      point-vs-full differential lane.

## 5. Gate / C / roborev (flow-closer)
- [ ] `--lite` each fix round (summary-file redirect); rust-reviewer + roborev on the lite-green diff.
- [ ] flow-closer: ONE full `scripts/agent-gate.sh` PASS (paste SUMMARY), `-D warnings` clean, no
      `unwrap()`/`expect()` in library code; C (spec-auditor) PASS on every requirement with a
      public-surface test as evidence; roborev clean → merge → `openspec archive`.
