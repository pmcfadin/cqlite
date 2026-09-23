# Proposal: CQLite testing strategy — layered, cost-tiered, enforceable (issue #4259)

**Milestone:** unmilestoned (program epic; children scheduled individually) · **Priority:** P1 ·
**Routing:** design-driven — this defines *how every future test is shaped*, what each CI tier may
contain, and what the project publicly claims about its own verification. · **Issue:** #4259 (epic) ·
**Children:** #4255, #4256, #4257, #4258 + three to be filed from `tasks.md` ·
**Source:** owner review 2026-09-22 of an external Apache Cassandra testing strategy (cursor-read work:
Harry-randomized schemas, parameterized matrices, named anchors, differential testing), mapped against
CQLite by four independent reviews.

## Why

CQLite's correctness story is strong but **accreted, not designed**. Its best rules exist as incident
write-ups — five "blind spots" in `CLAUDE.md`, the two-oracle section of the validation playbook,
zero-test guards in the gate — and each arrived after a bug. What is missing is the part that tells a
contributor **how to shape a new test** so it does not become the next incident. The review found:

1. **Test shape defaults to one-off files.** `cqlite-core/tests/` holds **417** test targets, **312** of
   them `issue_*`. Each is a separately linked binary under nextest; one-off files are also the reason a
   fixture is rebuilt per test (188 test files build their own temp SSTables). Nothing says "add a row to
   an existing matrix" is the default.
2. **The differential contract is implicit.** `point_vs_full_differential.rs` never checks
   `access_path` / `is_targeted()` (#4255), has no declared expected outcome per case, and runs a full
   scan through the process-global `GlobalKeyOffsetCache` before the point leg — so the point leg may
   never exercise its cold path. Suites self-test their *comparator*, never the *path*.
3. **Coverage of interacting axes is bug-driven.** Compression × reverse × BIG is covered because
   #1293 forced it; compression × reverse × BTI is not covered at all; the point-vs-full corpus has no
   reverse cases.
4. **There is no breadth engine.** Every oracle fixes one schema per fixture. The only history generator
   is a proptest strategy inside the merge unit tests (`merge/mod.rs:5803`) that never touches a real
   writer.
5. **Tiers exist, budgets do not.** PR / nightly / weekly lanes exist, but nothing caps what a tier may
   cost, so the owner's concern — adopting more testing backs up CI — has no structural answer today.

## What changes

A single written strategy with five layers, each backed by an enforceable contract:

- **Matrices** — fixture axes vs query axes; fixtures built once per fixture tuple via a
  cross-process disk cache (nextest runs each test in its own process, so an in-memory static cannot
  share); pairwise coverage of query axes; full product only on **named** interacting pairs.
- **Named anchors** — a new test file is justified only by a second invariant, a memorialized bug, or a
  sequence; otherwise it is a matrix row.
- **Differential contract** — a shared test-support type that makes a differential case impossible to
  write without a declared expected outcome and an engagement check; a cold new-path leg; one
  end-to-end fault-injection meta-test per suite.
- **Randomized breadth** — seeded random schema + mutation history; CQLite self-consistency nightly,
  Cassandra-written oracle weekly; failures file issues and never block merges; shrunk repros become
  committed regression tests. (Generator design is its own change under #4257.)
- **Tiers + budgets** — PR / nightly / weekly as nextest profiles; **no randomness in the PR tier**;
  per-tier time budgets that FAIL loudly when exceeded.

Plus: a canonical agent doctrine page, a slimmed `CLAUDE.md` Testing section that links it, and the
public "How CQLite Is Tested" docs section (#4258) with a known-blind-spots page.

## Cost posture (owner constraint)

Adoption SHALL NOT increase PR-tier wall-clock beyond its measured baseline + the declared budget
headroom. Randomized testing never runs in the PR tier. The expected PR-tier effect is **neutral to
negative** (fewer rebuilt fixtures, and — if the measurement in task 6 pays — fewer linked binaries).

## Non-goals

- **The random schema/history generator's detailed design** — specced separately when #4257 is picked
  up (owner ruling 2026-09-22: contracts now, generator later).
- **Mandated migration of existing tests.** The matrix-first rule applies to new work; consolidating
  existing `issue_*` targets is gated on a measurement (task 6) and only proceeds if it pays.
- **Mutation-testing tooling** (`cargo-mutants`) as a gate component.
- **Fixing unrelated red CI** (`ci.yml` on main, #4058).
- **Coverage-driven test pruning.** Deferred; revisit when the PR tier approaches its budget.
- Any change to the gate-of-record contract beyond the budget report/FAIL line (task 5).

## Doctrine impact

- New canonical page `website/src/content/docs/agents-developing/testing-doctrine.md`; `CLAUDE.md`
  "Testing" shrinks to the hard rules + a link (the five blind spots move to the doctrine page).
- Public docs gain a top-level "How CQLite Is Tested" section directly under User Docs.
- `roborev-findings` and `rust-reviewer` gain the anchor-vs-matrix and differential-contract checks.
- No change to the no-heuristics mandate, the version floor, or the #1406 write claim boundary.

## Impact

- `cqlite-core/tests/support/` — new `matrix`, `fixture_cache`, `differential` modules.
- `cqlite-core/src/` — two debug-only test seams (cache invalidation reachable from tests;
  fault injection on the point path), following the `CQLITE_TTL_NOW_OVERRIDE_SECS` precedent.
- `.config/nextest.toml` — `pr` / `nightly` / `weekly` profiles.
- `scripts/agent-gate.sh` — a budget report + FAIL line in the SUMMARY.
- `.github/workflows/` — nightly/weekly burn jobs attached to existing scheduled lanes.
- `website/` — new docs section; `CLAUDE.md`, doctrine pages.
