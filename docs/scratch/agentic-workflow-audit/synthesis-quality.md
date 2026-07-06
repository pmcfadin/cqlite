# Synthesis — Quality/Correctness Chain (gate → C → roborev → CI → parity tiers)

Scope: where the chain actually catches defects vs performs assurance theater, where it silently
passes, what is triple-checked while something else is unchecked, and the minimal chain that keeps
the same assurance. Grounded in the eight gatherer files + spot-checks of `pr-gate.yml`,
`agent-gate.sh` fixture policy, and parity workflow triggers.

## Findings

**F1 — The "gate of record" is honor-enforced, not CI-enforced.** The only CI check named `required`
(`.github/workflows/pr-gate.yml`, verified) runs: workflow-policy lint, `cargo fmt`, `cqlite-core`
clippy, `cqlite-core --all-features` build, and `cqlite-core --lib` tests **with 4 groups explicitly
skipped** (`select_integration_tests`, `test_database`, `test_batch_operations`, `plan_cache_tests`),
comment says "without Docker/datasets." It runs **zero** integration, CLI, binding, smoke, or parity
tests. The comprehensive `scripts/agent-gate.sh` (21 components) is a **local script** whose PASS is
asserted by an agent pasting a `==== AGENT-GATE SUMMARY ====` block. Nothing in branch protection
verifies that block ran, was truthful, or passed. Merge-on-green (2026-07-06 doctrine) trusts the
pasted artifact. **The real merge gate is agent honor + a thin fmt/clippy/unit subset.**

**F2 — Roborev is the actual workhorse defect-catcher by volume, and it is non-deterministic.**
Telemetry (`telemetry-analysis.md`): **608 roborev findings across 174 issues** (median 1, p90 10,
max 40); issues with findings average **2.71 rework rounds vs 0.17 without**. Roborev drives 334 of
the rework rounds and is the #1–2 weighted failure driver (score 860–1216). It is an LLM reviewer:
powerful, but honor-run, un-reproducible, and **findings carry no severity** (blocker vs nit), so
cosmetic findings force the same expensive re-gate as real bugs.

**F3 — The deterministic layer catches the most gate failures but is the cheapest and is
triple-redundant.** 151/325 gate runs fail (**46%**; first-time pass ~54%). Most of that is
fmt/clippy/unit — exactly the layer that runs **three times**: `--lite` every fix round, full gate
once, and `pr-gate.yml` in CI. High redundancy on the cheapest, lowest-severity signal.

**F4 — C (spec-audit) is design-only, post-gate, linear, and uninstrumented.** Runs on **27% of
issues** (47/174 design-routed); the other 73% oracle work never invokes it (`agents-and-skills.md`).
It is a serial post-gate blocker (10–15 min) whose findings, if they need a src change, force a
re-gate (`doctrine-and-process.md` friction #8). **No telemetry records unmet-verdict rate** — there
is no evidence it catches defects the gate/roborev miss. Strongest theater candidate.

**F5 — The crown-jewel correctness machinery runs POST-MERGE / nightly, never blocking a PR.**
Byte-for-byte compaction parity, live Cassandra readback, sstabledump parity, exhaustive
regeneration are nightly/Docker tiers (`docker-correctness.md`, `github-actions.md`): nightly
`gate.yml` (full agent-gate re-run on `main`), `exhaustive-regeneration.yml` (weekly), 240-min full
`sstabledump-parity-gate.yml`. `sstabledump-parity-gate.yml` and `smoke-tests.yml` *do* trigger on
`pull_request` (verified) but **path-filtered/conditional** and their "required"-ness lives in GitHub
Settings UI, not code — unauditable and not provably blocking. The most sophisticated correctness
layer catches defects **~24h after merge**, on `main`, as a backstop — not a gate.

**F6 — Silent-skip surface: a green full-gate SUMMARY can mean zero correctness was validated.**
`agent-gate.sh` sets `CQLITE_DATASETS_ROOT` default to the *checkout's own* `test-data/datasets`
(line 303), whose `*-Data.db` binaries are gitignored. In a worktree (which by doctrine lacks the
binaries) the parity/smoke/compaction components **SKIP** (loud at component level) but the overall
gate still returns **PASS**. A worker who forgets to point the env var at the main checkout pastes a
PASS SUMMARY whose parity lines read SKIP. Correctness validated = 0; the artifact reads green
(`agents-and-skills.md` friction #5, `gate-and-scripts.md`). Fail-closed (`CQLITE_REQUIRE_FIXTURES=1`)
is applied per-component only *when datasets are present*, not to enforce their presence.

**F7 — The headline byte-for-byte compaction parity is never enforced pre-merge.** Compaction legs
need external JDK/gradle, not containerized; agents run `--skip-compaction` locally
(`docker-correctness.md`). It is validated **only** in the nightly Docker lane, **advisory** tier. The
v0.12 flagship correctness property has zero pre-merge enforcement.

**F8 — Redundancy inversion (the core imbalance).** Checked 3×: fmt/clippy/core-lib-tests. Checked
2× (full local + nightly, 0× required CI): integration, CLI, binding, smoke/parity-vs-goldens.
Checked 1× (nightly, post-merge, advisory): byte-for-byte compaction, exhaustive regeneration. The
**cheapest, lowest-severity signal has the most redundancy; the highest-value correctness signal has
the least and runs latest.**

## Recommendations (ranked; payoff / cost)

**R1 — Make the enforced pre-merge check actually gate correctness, OR verify the full-gate SUMMARY
server-side.** The parity/smoke lanes already exist (`sstabledump-parity-gate.yml` smoke tier,
`smoke-tests.yml`, both PR-triggered). Add the smoke parity lane (run with
`CQLITE_REQUIRE_FIXTURES=1` on the CI dataset subset) to **branch-protection required checks** and
commit the required-check list (F1/F5). *Payoff: the only thing that would actually block a
correctness regression from merging — today nothing does. Cost: ~30–45 min CI per PR; mitigate with
path filters (run only when `cqlite-core/src/storage`, write paths, or parsers change).*

**R2 — Fail-closed the gate-of-record on absent datasets.** Add `AGENT_GATE_REQUIRE_FIXTURES`
default-ON for the full (non-`--lite`) run so parity/smoke/compaction **FAIL** (not SKIP) when
`CQLITE_DATASETS_ROOT` is unset or empty (F6). *Payoff: kills "green SUMMARY, SKIP parity"
false-assurance — the single most dangerous silent pass. Cost: ~15-line script change; workers must
`fetch-datasets.sh` once (already doctrine). Keep `--lite` lenient.*

**R3 — Stratify roborev findings by severity; gate only on blockers.** 608 findings → 334 rework
rounds with no blocker/nit split (F2); rework is the top weighted failure driver (score 880–1336).
Emit severity from roborev, let non-blocking nits ride to a follow-up. *Payoff: directly cuts the #1
cost driver and the p90=3/max=9 gate-run tail. Cost: roborev output-schema + telemetry counter
change; risk: trusts the LLM's severity call (keep "blocker" conservative).*

**R4 — Replace full-gate re-run per roborev round with a scoped re-verify.** Each roborev fix round
today re-runs the 12–25 min full gate (`doctrine-and-process.md`); `#1892 --delta` already proved the
principle for test/docs-only. Extend: a src-touching roborev-fix re-certs with `--lite` + the diff's
scoped parity, deferring the **one** mandatory full gate to immediately pre-merge. *Payoff: removes
most of the 151 failed/re-verify gate runs (46% of all gate executions). Cost: small; the
single-full-gate-before-merge rule (already doctrine) backstops cross-component regressions.*

**R5 — Instrument C's catch rate before keeping it.** Add an unmet/partial-verdict counter to the
telemetry ledger (F4). If unmet≈0 over ~30 design issues, demote C to a checklist inside roborev.
*Payoff: removes a 10–15 min linear post-gate blocker on 27% of issues if it is theater. Cost: one
counter now; a few weeks of data before cutting.*

**R6 — Promote byte-for-byte compaction parity to a path-filtered required check on write-path
diffs.** Build a prebuilt Cassandra+JDK CI image so the compaction leg runs pre-merge only when
write/compaction source changes (F7). *Payoff: the flagship correctness claim gets enforced before
merge on exactly the diffs that can break it, not 24h later. Cost: CI image build + ~10–20 min on
write-path PRs only.*

## Risks / tradeoffs

- **R1/R6 reintroduce CI wait** that merge-on-green removed. Mitigate with aggressive path filters so
  most PRs (docs, bindings, non-storage) skip the parity lanes entirely.
- **R2** could block legitimate minimal-checkout iteration — scope strictly to the gate-of-record;
  `--lite` stays lenient by design.
- **R3** trusts non-deterministic LLM severity; keep the blocker bar conservative and let the final
  full gate + required CI catch anything mis-graded as a nit.
- **R5** risks losing intent-conformance on design work if C is cut prematurely — measure first, and
  only demote for issue classes where unmet-rate is provably ~0.
- Branch-protection changes (R1) are UI state today; committing the required-check list makes the
  chain auditable but also makes a mistaken edit a merge-blocker — worth it for provability.

## Minimal chain that keeps the same assurance

1. **Deterministic layer** (fmt/clippy/unit): enforced **once** in required CI is sufficient. `--lite`
   local reruns are fine (speed), but drop the expectation that triple-running it adds assurance (F3).
2. **Correctness layer** (integration + smoke/parity-vs-goldens): must run **at least once, enforced,
   pre-merge, fail-closed on datasets** (R1+R2). This is the current hole — it exists only as a local
   honor artifact or a post-merge nightly.
3. **Roborev**: keep — it is the highest-yield catcher — but severity-gate (R3) and scope its
   re-verify (R4).
4. **C**: measure (R5); likely demote for the 73% oracle work where it already never runs.
5. **Nightly Docker / exhaustive regeneration**: keep as the genuine backstop for byte-drift and
   full-corpus regeneration that legitimately cannot run per-PR (F5) — plus write-path compaction as a
   path-filtered required check (R6).

Net: move one enforced correctness check *before* merge and fail it closed on missing data; stop
paying triple for cheap deterministic checks and single-for-nightly on the expensive correct ones.
