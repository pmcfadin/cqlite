# Tasks — read-path-forcing-knob (#1918)

Each task names the public surface it exercises. Gate/C/roborev steps are the standing implement loop
(`--lite` each fix round; review-first; flow-closer runs the ONE full gate → C → roborev → merge).

## 1. Knob surface + resolution
- [ ] Add `ReadPathMode { Auto, Point, Full }` and `QueryConfig.forced_read_path: Option<ReadPathMode>`
      (`config.rs`). Public surface: `QueryConfig`.
- [ ] Resolve mode once via `OnceLock`: config → `CQLITE_READ_PATH` env (case-insensitive) → `Auto`;
      unrecognized value → distinct `Error`. Public surface: the resolver fn + `Error` variant.
- [ ] Unit tests: unset=Auto, env parse, config-over-env precedence, invalid-value loud error.
      Exercises: the resolver + `QueryConfig`.

## 2. Single forcing gate over the classifier
- [ ] Add `apply_forcing(outcome, mode) -> Result<ForcedOutcome>` wrapping
      `classify_partition_lookup`'s return; add `FallbackReason::ForcedFullScan` and
      `Error::ForcedReadPathUnavailable { forced, reason }`. Public surface: `AccessPath`/`FallbackReason`
      (closed-enum contract), `Error`.
- [ ] Wire the wrapper at all five call sites (`execute.rs` metadata/schemaless/materializing,
      `streaming.rs`, `stream_agg.rs`) — one helper, no per-site policy. Exercises: `Database::execute`
      (materializing + streaming SELECT surfaces).
- [ ] `full` records `FallbackFullScan{ForcedFullScan}`; `point` fails closed on `Fallback` up front and
      on `engaged == false` post-call. Exercises: `AccessPath::last()` probe.
- [ ] Tests: `full` on a targeted query records forced fallback + rows == `auto`; `point` on partial-pk
      errors distinctly; `point` on full-pk stays targeted. Exercises: `Database::execute` + probe.

## 3. Differential-equality lane
- [ ] New integration target `cqlite-core/tests/point_vs_full_differential.rs`: run the eligible corpus
      query matrix under forced `point` and forced `full`, assert normalized (rows/values/order)
      equality; pin `now` via `CQLITE_TTL_NOW_OVERRIDE_SECS`; fail-closed under `CQLITE_REQUIRE_FIXTURES=1`,
      SKIP-loud when the corpus is absent. Exercises: `Database::execute` under both forced modes.
- [ ] Include multi-generation (`ct_multi_sstable_merge` and peers), tombstone, and TTL fixtures.
- [ ] Verify the lane by seeding a divergence locally (break one path, confirm red, revert) —
      regression-test-verification doctrine; record the demonstration in the PR.
- [ ] Wire into the existing `integration-tests` gate tier (design.md placement); OWNER-FORK: promote to
      a dedicated component only if independent SUMMARY visibility is chosen.

## 4. Observability + docs
- [ ] If cheap: surface the `AccessPath` label + a "forced" marker in CLI `--explain`
      (`cqlite-cli/src/commands/query.rs`). Else split to a follow-up issue. Exercises: `--explain`.
- [ ] Document `CQLITE_READ_PATH` in CLI/user docs as a test/debug-only control (values, `point`
      fail-closed behavior, not a perf recommendation).
- [ ] Update CLAUDE.md two-oracle note + `agents-developing/` validation-playbook to name the
      point-vs-full differential lane.

## 5. Gate / C / roborev (flow-closer)
- [ ] `--lite` each fix round (summary-file redirect); rust-reviewer + roborev on the lite-green diff.
- [ ] flow-closer: ONE full `scripts/agent-gate.sh` PASS (paste SUMMARY), `-D warnings` clean, no
      `unwrap()`/`expect()` in library code; C (spec-auditor) PASS on every requirement with a
      public-surface test as evidence; roborev clean → merge → `openspec archive`.
