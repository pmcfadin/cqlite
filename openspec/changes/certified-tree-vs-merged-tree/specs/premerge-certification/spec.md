# premerge-certification — delta for certified-tree-vs-merged-tree (issue #3650, SLICE 1)

**Architecture note (read this first).** `scripts/flow/premerge-assert.sh` is the merge-point guard. Since
#3465 (PR #3646) it proves two things: the PR head still equals the certified sha, and a full
`AGENT-GATE SUMMARY` with `RESULT: PASS` and `tree-integrity: PASS` exists whose `commit:`/`tree-start:`
cover that sha. It does **not** prove the diff was certified against the `main` it will join — a
squash-merge composes the diff with main's *current* tip, so for any stale-based PR **the certified tree
and the merged tree are different objects**. Its own output says so, on three `PREMERGE: SCOPE` lines
naming #3650.

**This delta is SLICE 1 of 2 and changes NO verdict.** It ships the *information source* the owner
sequenced first: a non-blocking advisory reporting how far a PR's base is behind `origin/main` and how much
of that churn lands in the diff's blast radius. Slice 2 — a merge-result gate mode and the fail-closed
enforcement that consumes this — is filed separately, and the #3465 scope disclaimer is **retained**
until it lands.

**Acceptance-criterion → requirement map** (issue #3650):

| AC | Requirement(s) | Slice |
|---|---|---|
| AC1 — owner decides the freshness bound | *Satisfied outside this spec* by the owner ruling of 2026-08-30T23:59:56Z (blast-radius staleness); mechanized by ADDED *The blast radius is path intersection ∪ a declared gate-global set* | 1 |
| AC2 — gate of record runnable against the merge result, distinguishable summary | — | **2** |
| AC3 — `premerge-assert` requires merge-result certification, disclaimer updated | ADDED *Slice 1 changes no verdict and retains the #3465 scope disclaimer* (the retention half) | **2** (requirement half) |
| AC4 — a stale-based PR with a head PASS that fails against main is refused | — | **2** |
| AC5 — the advisory is non-blocking and cannot become a false certification | ADDED *The advisory cannot be read as a certification*, ADDED *An unmeasurable scan is UNMEASURED and is fail-closed by contract*, ADDED *Slice 1 changes no verdict …* | 1 |
| AC6 — mutation-checked, per #3465's precedent | ADDED *The advisory's blast-radius definition is mutation-checked against the motivating case* | 1 |

## ADDED Requirements

### Requirement: The base-staleness advisory reports N commits behind and M in the blast radius

`scripts/flow/base-staleness.sh` SHALL, for a branch in the current checkout, report the number of commits
`N` on `origin/main` that are not reachable from the branch's **merge-base** with `origin/main`, and the
number `M` of those commits that touch the diff's blast radius. It SHALL print the merge-base it used, the
`origin/main` sha it measured, and that sha's commit date, so the measurement is attributable and its own
staleness is visible. It SHALL NOT fetch, mutate any ref, or write in the repository.

#### Scenario: A stale base with churn in the blast radius is reported as stale
- **GIVEN** a branch whose merge-base with `origin/main` is behind by at least one commit
- **AND** at least one of those commits touches a path in the diff's blast radius
- **WHEN** `base-staleness.sh` is run for that branch
- **THEN** it prints `behind <N> commits` with `N` equal to `git rev-list --count <merge-base>..origin/main`
- **AND** it prints a blast-radius count `M >= 1`
- **AND** its verdict line reads `STALE-RECOGNISED`
- **AND** it exits `4`

#### Scenario: An up-to-date base reports zero behind without claiming freshness
- **GIVEN** a branch whose merge-base with `origin/main` equals `origin/main`
- **WHEN** the advisory is run
- **THEN** it prints `behind 0 commits`
- **AND** its verdict line reads `NO-STALENESS-RECOGNISED` and NOT `FRESH` or `CLEAN`
- **AND** it exits `0`

#### Scenario: The base is the merge-base, never the base ref's tip
- **GIVEN** a branch whose `origin/main` has advanced past its branch point
- **WHEN** the advisory is run
- **THEN** the reported base is `git merge-base origin/main HEAD`
- **AND** the printed base sha is that merge-base, labelled as such

*Verified by:* `scripts/tests/test_base_staleness.sh` (behind-count, verdict-token, merge-base cases).

### Requirement: The blast radius is path intersection ∪ a declared gate-global set

The blast radius of a diff SHALL be the union of (a) the paths the diff itself touches and (b) a
**gate-global** path set — content that can change any gate's verdict regardless of the diff. The
gate-global set SHALL be hard-coded in one location in the script with **no environment override**, since
an override is settable by the party it constrains.

#### Scenario: A commit sharing no path with the diff still stales it via the gate-global set
- **GIVEN** a diff touching only paths under `cqlite-core/src/**` and `cqlite-core/tests/issue_*.rs`
- **AND** a commit behind the base touching `.config/nextest.toml` and no path in the diff
- **WHEN** the advisory is run
- **THEN** the verdict is `STALE-RECOGNISED`
- **AND** the output names the gate-global path that matched

#### Scenario: Unrelated churn does not stale a certification
- **GIVEN** commits behind the base touching neither the diff's paths nor any gate-global path
- **WHEN** the advisory is run
- **THEN** those commits are counted in `N` but NOT in `M`
- **AND** the verdict is `NO-STALENESS-RECOGNISED`

*Verified by:* `scripts/tests/test_base_staleness.sh` (gate-global match case; unrelated-churn case).
*Rationale (measured):* `docs/round-artifacts/issue-3650-blast-radius-measurements.md` — on PR #3362 the
culprit commit and the diff share no path, so intersection alone is unsound; intersection ∪ gate-global
fires on 22 of 107 commits behind (21%), so unrelated churn still does not stale.

### Requirement: The advisory cannot be read as a certification

The advisory's output SHALL NOT contain the tokens `PASS`, `OK`, or `RESULT:` in any run, since those are
the verdict vocabulary of `AGENT-GATE *SUMMARY`, `ROBOREV REVIEW SUMMARY` and `PREMERGE:` blocks. Its
lines SHALL carry the distinct prefix `BASE-STALENESS:`. A blast-radius count of zero SHALL be printed as
`0 RECOGNISED`, never as a bare `0`. Every run SHALL print its own non-exhaustiveness: the scan is path
intersection ∪ gate-global and is **not** a dependency closure, so a commit changing an item the diff
calls without touching the diff's paths or a gate-global path is reported as not staling.

#### Scenario: No verdict token from another artifact appears in any run
- **GIVEN** any of the stale, non-stale, or unmeasurable cases
- **WHEN** the advisory is run
- **THEN** its complete output contains no occurrence of `PASS`, `OK`, or `RESULT:`

#### Scenario: A zero blast radius is affirmative, not bare
- **GIVEN** a run in which no commit behind the base touches the blast radius
- **WHEN** the advisory prints the blast-radius line
- **THEN** it reads `0 RECOGNISED` and names the scan's scope
- **AND** the same run prints the `NON-EXHAUSTIVE` statement

*Verified by:* `scripts/tests/test_base_staleness.sh` (vocabulary case; `0 RECOGNISED` case;
non-exhaustiveness-present case).

### Requirement: An unmeasurable scan is UNMEASURED and is fail-closed by contract

Where the scan cannot be performed — `origin/main` absent, no merge-base between the branch and
`origin/main`, or a git invocation failing — the advisory SHALL report `UNMEASURED` with a cause and exit
`5`. It SHALL NOT report `0`, `NO-STALENESS-RECOGNISED`, or exit `0`. The script header and this spec
SHALL state that a consumer MUST treat `UNMEASURED` as stale, never as fresh.

#### Scenario: A missing origin/main is unmeasurable, not clean
- **GIVEN** a checkout with no `origin/main` ref
- **WHEN** the advisory is run
- **THEN** the verdict line reads `UNMEASURED` and names the cause
- **AND** it exits `5`
- **AND** the output contains neither `NO-STALENESS-RECOGNISED` nor a bare blast-radius `0`

#### Scenario: No merge-base is unmeasurable
- **GIVEN** a branch with no merge-base with `origin/main`
- **WHEN** the advisory is run
- **THEN** the verdict is `UNMEASURED` and it exits `5`

*Verified by:* `scripts/tests/test_base_staleness.sh` (missing-ref case; no-merge-base case; the
"exit 5 is never exit 0" assertion).

### Requirement: Slice 1 changes no verdict and retains the #3465 scope disclaimer

`premerge-assert.sh` SHALL print the advisory's finding on `PREMERGE: ADVISORY` lines and SHALL NOT alter
its exit code or any refusal on account of it. An advisory that fails to run, is absent, or reports
`UNMEASURED` SHALL be reported on those lines and SHALL NOT be fatal in slice 1. The three existing
`PREMERGE: SCOPE` lines naming #3650 SHALL be retained, since slice 1 does not close the gap they
disclose; they MAY be extended to point at the advisory.

#### Scenario: A stale-based PR is still merged, with the advisory printed
- **GIVEN** a PR whose head equals the certified sha and whose gate of record is a full `RESULT: PASS`
- **AND** whose base is behind with churn in its blast radius
- **WHEN** `premerge-assert.sh` runs
- **THEN** it exits `0` with `PREMERGE: OK`
- **AND** its output carries `PREMERGE: ADVISORY` lines reporting `STALE-RECOGNISED`
- **AND** its output still carries the `PREMERGE: SCOPE` lines and the literal `#3650`

#### Scenario: A broken advisory cannot fail the assert in slice 1
- **GIVEN** an otherwise-passing `premerge-assert.sh` invocation
- **AND** an advisory that is absent or exits non-zero for any reason
- **WHEN** `premerge-assert.sh` runs
- **THEN** it still exits `0`
- **AND** the advisory's unavailability is reported on a `PREMERGE: ADVISORY` line

*Verified by:* `scripts/tests/test_premerge_assert.sh` (advisory-printed case; broken-advisory
non-fatal case; extended Case 39 for the retained SCOPE wording).

### Requirement: The advisory's blast-radius definition is mutation-checked against the motivating case

The test suite SHALL contain a pinned regression case reproducing PR #3362's shape — a diff sharing no path
with a commit behind the base that touches a gate-global path — asserting `STALE-RECOGNISED`. That case
SHALL red if the gate-global half of the definition is removed. The suite SHALL additionally plant that
defect in a copy of the script and assert the suite catches it, so the guard is observed to fire rather
than assumed to.

#### Scenario: Removing the gate-global set reds the suite
- **GIVEN** a copy of `base-staleness.sh` with the gate-global set emptied
- **WHEN** the test suite is run against that copy
- **THEN** the motivating-case test FAILS
- **AND** the failure names the blast-radius definition

#### Scenario: The suite is wired into the gate
- **GIVEN** the full `scripts/agent-gate.sh`
- **WHEN** the `tooling-tests` component runs
- **THEN** `scripts/tests/test_base_staleness.sh` is executed
- **AND** a failing assertion in it makes the full gate FAIL

*Verified by:* `scripts/tests/test_base_staleness.sh` (planted-mutant case) and its registration in
`run_tooling_tests` in `scripts/agent-gate.sh`.

### Requirement: Doctrine states what the advisory does and does not prove

`CLAUDE.md`'s `premerge-assert` section, the script header, `.claude/agents/flow-closer.md` and
`.claude/skills/flow-address/SKILL.md` SHALL describe the advisory, its non-blocking status in slice 1,
the `UNMEASURED`-is-stale consumer contract, and the declared non-exhaustiveness of the blast radius. They
SHALL continue to state that a `PREMERGE: OK` does not prove certification against current `main`.

#### Scenario: Doctrine does not overclaim
- **GIVEN** the doctrine text after this change
- **WHEN** a reader looks for what a `PREMERGE: OK` proves
- **THEN** it still states the merge-result gap is open and names slice 2's issue
- **AND** it does not describe the advisory as a certification or as blocking

*Verified by:* the doctrine diff, and `scripts/tests/test_premerge_assert.sh` Case 39 pinning the retained
`PREMERGE: SCOPE` / `#3650` / `does NOT prove` literals.
