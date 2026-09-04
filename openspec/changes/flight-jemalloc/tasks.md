# Tasks — flight-jemalloc (issue #3997)

Ordered. Groups 1–2 are the mechanism (small), 3 is the measurement (the expensive part, on the
#3551 rig `ip-172-31-7-163`), 4 is the decision, 5 is doctrine/coverage. Commit after every group.

## 0. Premises — re-confirm on the box, STOP if false

- [ ] 0.1 No `#[global_allocator]` in any non-test production file (`git grep global_allocator -- '*.rs'`
      shows only `cfg(test)` core sites, dhat test binaries, examples). Design rests on this.
- [ ] 0.2 `bindings/**/Cargo.toml` and `cqlite-core/Cargo.toml` do not depend on `cqlite-flight`.
- [ ] 0.3 `libjemalloc.so.2` used by #3551 is jemalloc 5.3 (`ldconfig -p`, or the report's recorded
      version) — the `tikv-jemallocator` pin must match the major measured.

## 1. Mechanism (R1, R2) — surface: `cqlite-flight` binary

- [ ] 1.1 `cqlite-flight/Cargo.toml`: `tikv-jemallocator = { version = "0.6", optional = true }`,
      feature `jemalloc = ["dep:tikv-jemallocator"]`. **`default` stays `[]` in this group.**
- [ ] 1.2 `cqlite-flight/src/main.rs`: `#[cfg(all(feature = "jemalloc", target_os = "linux"))]
      #[global_allocator] static GLOBAL: tikv_jemallocator::Jemalloc = …;` plus one
      `const ALLOCATOR: &str` derived from the same cfg.
- [ ] 1.3 `--version` prints `allocator: <ALLOCATOR>`; startup info line gains `allocator=`.
- [ ] 1.4 Test `cqlite-flight/tests/issue_3997_allocator_surface.rs` (R2.1): runs the built binary
      with `--version` under the active feature set and asserts the line. Must pass in BOTH states.
- [ ] 1.5 `scripts/tests/test_flight_allocator_link.sh` (R1.1/R1.2): symbol check on Linux;
      SKIP-with-reason off-Linux (never a vacuous PASS — print the platform).
- [ ] 1.6 `--lite` green; commit.

## 2. Confinement (R4, R5) — surface: `tooling-tests`

- [ ] 2.1 `scripts/tests/test_flight_allocator_confinement.sh`: exactly-one-production-site assert
      + no `tikv-jemallocator` in core/cli/bindings manifests + dependents link the lib target.
      Register it where `tooling-tests` discovers scripts; confirm it is EXECUTED (census line).
- [ ] 2.2 `--lite` green; commit; open the PR (draft) so the rig work below has a head to certify.

## 3. Measurement (R3.1, R3.3, R6.1) — rig `ip-172-31-7-163`, #3551 method

- [ ] 3.1 Build two release binaries from the SAME commit: `--no-default-features` (arm A) and
      `--features jemalloc` (arm E); record both sha256 in the round metadata.
- [ ] 3.2 `scripts/perf/ws0-3551-abc.sh`: add arm `E` (= arm A flags + `--flight-binary <E>`);
      `ws0_abc_aggregate.py`: allow E as the single permitted cross-arm binary exception (R3.3 test
      in `test_ws0_abc_driver_guards.sh`).
- [ ] 3.3 `ws0_flight_arm.py`: sample `VmHWM`/`VmRSS` of the server pid at scan end; add both to the
      aggregate tables.
- [ ] 3.4 Run ≥3 interleaved sets of A/E at N=1 (pin `2,10`, quiescence-gated as #3551) and ≥3 pairs
      at the admission ceiling. Quiescence + pair-control rules unchanged.
- [ ] 3.5 `docs/reports/ws0-3997-report.md` + `docs/reports/ws0-3997-artifacts/`: tables, verdict
      per the pre-registered criterion, byte basis, fixture sha256, binary sha256s. Post the verdict
      on #3023 (single reporting surface) and #3997.

## 4. Decision (R3.2)

- [ ] 4.1 SHIP-default → `default = ["jemalloc"]` in `cqlite-flight/Cargo.toml`, commit message cites
      the report path + median Δ. Confirm `flight-loadgen` still builds (it inherits default
      features of the lib target but installs nothing).
- [ ] 4.2 SHIP-opt-in or DO-NOT-SHIP → leave `default = []`; for DO-NOT-SHIP remove the feature and
      keep the report; close #3997 as an honest negative.

## 5. Doctrine + endgame

- [ ] 5.1 `docs/development/dev-cookbook.md`: how to build with/without the allocator; how to read
      `allocator:` in `--version`. `docs/observability/` if the startup line is documented there.
- [ ] 5.2 Helm/Trino deployment notes: state that the release binary carries the allocator; nothing
      to configure.
- [ ] 5.3 Full gate ONCE (`AGENT_GATE_SUMMARY_FILE` redirect) → C intent audit vs this spec →
      roborev (`scripts/flow/roborev-review.sh --agent … --model …`) clean → `premerge-assert` →
      `gh pr merge --auto --squash --delete-branch` → flow-finalize (telemetry via PR-in-worktree).
