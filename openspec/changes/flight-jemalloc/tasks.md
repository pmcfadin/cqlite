# Tasks — flight-jemalloc (issue #3997)

Ordered. Groups 1–2 are the mechanism (small), 3 is the measurement (the expensive part, on the
#3551 rig `ip-172-31-7-163`), 4 is the decision, 5 is doctrine/coverage. Commit after every group.

## 0. Premises — re-confirm on the box, STOP if false

- [x] 0.1 No `#[global_allocator]` in any non-test production file (`git grep global_allocator -- '*.rs'`
      shows only `cfg(test)` core sites, dhat test binaries, examples). Design rests on this.
- [x] 0.2 `bindings/**/Cargo.toml` and `cqlite-core/Cargo.toml` do not depend on `cqlite-flight`.
- [x] 0.3 `libjemalloc.so.2` used by #3551 is jemalloc 5.3 (`ldconfig -p`, or the report's recorded
      version) — the `tikv-jemallocator` pin must match the major measured.

## 1. Mechanism (R1, R2) — surface: `cqlite-flight` binary

- [x] 1.1 `cqlite-flight/Cargo.toml`: `tikv-jemallocator = { version = "0.6", optional = true }`,
      feature `jemalloc = ["dep:tikv-jemallocator"]`. **`default` stays `[]` in this group.**
- [x] 1.2 `cqlite-flight/src/main.rs`: `#[cfg(all(feature = "jemalloc", target_os = "linux"))]
      #[global_allocator] static GLOBAL: tikv_jemallocator::Jemalloc = …;` plus one
      `const ALLOCATOR: &str` derived from the same cfg.
- [x] 1.3 `--version` prints `allocator: <ALLOCATOR>`; startup info line gains `allocator=`.
- [x] 1.4 Test `cqlite-flight/tests/issue_3997_allocator_surface.rs` (R2.1): runs the built binary
      with `--version` under the active feature set and asserts the line. Must pass in BOTH states.
- [x] 1.5 `scripts/tests/test_flight_allocator_link.sh` (R1.1/R1.2): symbol check on Linux;
      SKIP-with-reason off-Linux (never a vacuous PASS — print the platform).
- [x] 1.6 `--lite` green; commit.

## 2. Confinement (R4, R5) — surface: `tooling-tests`

- [x] 2.1 `scripts/tests/test_flight_allocator_confinement.sh`: exactly-one-production-site assert
      + no `tikv-jemallocator` in core/cli/bindings manifests + dependents link the lib target.
      Register it where `tooling-tests` discovers scripts; confirm it is EXECUTED (census line).
- [x] 2.2 `--lite` green; commit; open the PR (draft) so the rig work below has a head to certify.

## 3. Measurement (R3.1, R3.3, R6.1) — rig `ip-172-31-7-163`, #3551 method

- [ ] 3.1 Build two release binaries from the SAME commit: `--no-default-features` (arm A) and
      `--features jemalloc` (arm E); record both sha256 in the round metadata.
- [x] 3.2 `scripts/perf/ws0-3551-abc.sh`: add arm `E` (= arm A flags + `--flight-binary <E>`);
      `ws0_abc_aggregate.py`: allow E as the single permitted cross-arm binary exception (R3.3 test
      in `test_ws0_abc_driver_guards.sh`).
- [x] 3.3 `ws0_flight_arm.py`: sample `VmHWM`/`VmRSS` of the server pid at scan end; add both to the
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

- [x] 5.1 `docs/development/dev-cookbook.md`: how to build with/without the allocator; how to read
      `allocator:` in `--version`. `docs/observability/` if the startup line is documented there.
- [x] 5.2 Helm/Trino deployment notes: state that the release binary carries the allocator; nothing
      to configure.
- [x] 5.3 Full gate ONCE (`AGENT_GATE_SUMMARY_FILE` redirect) → C intent audit vs this spec →
      roborev (`scripts/flow/roborev-review.sh --agent … --model …`) clean → `premerge-assert` →
      `gh pr merge --auto --squash --delete-branch` → flow-finalize (telemetry via PR-in-worktree).

---

## Lane status (worker lane `ip-172-31-5-53`, worktree `/data/lanes/lane-3997`)

**Done here:** groups 0, 1, 2, 5, and tasks **3.2/3.3** (the driver arm + the RSS sampling,
including the sampler's CALL SITE in `scripts/perf/lib-measure.sh` — it landed as a collector
with nothing calling it, which left R6.1 undecidable).

**NOT done, and NOT doable here — `3.1`, `3.4`, `3.5` and group `4`.** They require the #3551
rig `ip-172-31-7-163`; this lane is on `ip-172-31-5-53` and the rig is unreachable from it
(`ssh` → `Permission denied (publickey)`). So **R3.1, R3.2 and R6.1 are UNMET in this change**
and a `spec-auditor` (C) run against this spec must report them so. `default` stays `[]`, so
nothing ships the allocator to anyone: the feature is opt-in and inert until the rig verdict.

**Deviation from 3.2 as written, recorded deliberately.** The driver flag is `--bin-dir-e DIR`,
not `--flight-binary <E>`. No `--flight-binary` exists anywhere in the rig, and the digest the
aggregate reads for its cross-arm invariant is derived from `--bin-dir`
(`ws0_binaries.record_binary_provenance` off `$BIN`) — so a per-binary override would launch one
program and record another's digest, granting R3.3's exception against the wrong bytes. R3.3
names no flag, so this deviates from the task wording only, not from the requirement.

**Rig recipe for whoever picks up 3.1/3.4/3.5:** build `bins-A/` (all three binaries,
`--no-default-features`), then `bins-E/` = the `--features jemalloc` `cqlite-flight` plus
hardlinks/copies of `bins-A`'s `ws0-scan-bench` and `flight-loadgen`; the driver refuses the set
otherwise (that two-sided precondition is what earns arm E its exception). With `--bin-dir-e` the
set is **6 arms/round** (A,B,C0,C,D,E), not A/E only — there is no `--arms` selector on the
driver; the aggregate is then run `--arms A,E --baseline A`, and the driver prints that command.

**Rig-only confirmations nobody has made yet** (reasoned, not measured — do not treat as done):
a `tikv-jemallocator`-linked binary leaves **no** `libjemalloc` mapping, which is what lets arm E
run under `--flight-allocator system` and still pass `verify_flight_server_allocator`'s per-rep
`/proc/<pid>/{environ,maps}` check; real `VmHWM`/`VmRSS` magnitudes under load; the digest
precondition against real cargo output; and that `refuse_binaries_older_than_head` accepts two
separately-built bin dirs.

---

## Archive status — DEFERRED to #4120, with a second blocker recorded

**`openspec archive flight-jemalloc` was attempted on 2026-09-07 after PR #4117 merged
(`c8cf992c7`) and it correctly ABORTED, changing no files.** Two independent reasons:

1. **The change is not complete, by design.** Six tasks remain open — 3.1/3.4/3.5 (the linked A/E
   measurement) and 4.1/4.2 (the default decision) — all deferred to **#4120** under the lead's
   `req-3997-01` Q1 ruling, which merged #4117 as a mechanism-only slice with `default = []`. An
   archive is for a completed change, so this one stays ACTIVE until #4120 finishes it. #3997 also
   stays OPEN for the same reason. Archiving here would have recorded a decision nobody has made.

2. **A SEPARATE, INDEPENDENT BLOCKER that will still be there when #4120 is done, so it is
   recorded now rather than rediscovered then.** `openspec archive` reports:
   `Delta parsing found no operations for flight-allocator. Provide ADDED/MODIFIED/REMOVED/RENAMED
   sections in change spec.` The cause is the spec's header style: `specs/flight-allocator/spec.md`
   uses `## Requirement R1 — …` headers, whereas OpenSpec's delta parser wants
   `## ADDED Requirements` with `### Requirement: …` / `#### Scenario: …` beneath it. Peer changes
   in this repo use the parseable form (e.g. `openspec/changes/feature-matrix-gate-lanes/`,
   `openspec/changes/certified-tree-vs-merged-tree/`).

   **Not fixed from this lane on purpose.** The spec's CONTENT was sealed by the owner at Seam 1;
   converting it to delta headers is a structural rewrite of every requirement heading in a sealed
   artifact, and it is not a change a worker should make unilaterally at finalize time. Whoever
   completes this change decides: restructure into delta headers, or set `skip_specs: true` in the
   change's `.openspec.yaml` if the capability is judged already-captured. Either way the archive
   cannot succeed until that is settled.
