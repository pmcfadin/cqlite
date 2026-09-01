# premerge-certification — delta for gate-merge-result-premerge-assert (issue #3680, SLICE 2)

**Architecture note (read this first).** `scripts/flow/premerge-assert.sh` is the merge-point guard. Since
#3465 it proves the PR head still equals the certified sha and that a full `AGENT-GATE SUMMARY` with
`RESULT: PASS`, `tree-integrity: PASS` and `dirty: no` exists covering that sha. Since #3650 slice 1 it
also *reports*, non-blockingly, how far the base is behind `origin/main` and how much of that churn is in
the diff's blast radius. It still does **not** prove the diff was certified against the `main` it will
join. Four `PREMERGE: SCOPE` lines say so on every success path.

**This delta is SLICE 2 of 2 and is the enforcement.** It adds a gate mode that certifies the **merge
result**, requires that certification fail-closed when the advisory reports staleness, and retires the
scope disclaimer slice 1 deliberately retained.

**Acceptance-criterion → requirement map** (issue #3680):

| AC | Requirement(s) |
|---|---|
| AC1 — owner decides the freshness bound for the merge-result certification itself | *Owner decision (Seam 1)*; mechanized by ADDED *A merge-result certification is itself subject to the same staleness predicate* |
| AC2 — the gate of record can be run against the merge result | ADDED *The gate can certify the composed merge result* |
| AC3 — its summary is distinguishable, names the composed tip, neither block pastes as the other | ADDED *A merge-result summary is structurally distinguishable from a branch-head summary* |
| AC4 — `premerge-assert` requires it fail-closed on `STALE-RECOGNISED` | ADDED *A stale certification requires merge-result certification before merge* |
| AC5 — `UNMEASURED` treated as stale, with a test that reds otherwise | ADDED *Every non-affirmative advisory state is treated as stale* |
| AC6 — a stale-based PR whose head PASSes but whose merge result FAILS is refused | ADDED *A merge result that fails is refused even when the head certification passes* |
| AC7 — the #3465 disclaimer updated in all five locations, Case 39 extended | ADDED *Doctrine states what a `PREMERGE: OK` now proves* |
| AC8 — mutation-checked | ADDED *The enforcement is mutation-checked* |
| AC9 — demonstration planned where it must be, stated per half | ADDED *Doctrine states what a `PREMERGE: OK` now proves* (the scoping half) |

## ADDED Requirements

### Requirement: The gate can certify the composed merge result

`scripts/agent-gate.sh` SHALL accept a mode, as **argument 1**, that certifies the merge of `origin/main`
and the branch head rather than the branch head alone. It SHALL compose that merge into a **commit
object** whose tree is the merge result and whose parents are the `origin/main` tip and the branch head,
check that commit out in a scratch worktree, and run the full component set there. It SHALL NOT run
against an uncommitted composition: the certified tree must read `dirty: no`, because
`premerge-assert.sh` enforces that affirmatively and #2926 re-compares it at every component boundary.

When the merge cannot be composed — a conflict, or a git that cannot perform the composition — the mode
SHALL refuse with a named cause and SHALL stamp no certification. It SHALL NOT emit a passing summary for
a merge result that does not exist.

#### Scenario: A clean merge is composed and certified
- **GIVEN** a branch whose merge with `origin/main` is conflict-free
- **WHEN** the merge-result mode is run
- **THEN** it composes a commit whose tree equals `git merge-tree --write-tree origin/main <head>`
- **AND** the gate runs in a scratch worktree checked out at that commit
- **AND** the emitted summary reports `dirty: no`

#### Scenario: A conflicting merge is refused, not certified
- **GIVEN** a branch whose merge with `origin/main` conflicts
- **WHEN** the merge-result mode is run
- **THEN** it refuses with a cause naming the conflict
- **AND** no summary carrying `RESULT: PASS` is written for that run

#### Scenario: An unavailable composition capability refuses rather than degrades
- **GIVEN** a host whose git cannot perform `merge-tree --write-tree`
- **WHEN** the merge-result mode is run
- **THEN** it refuses with a cause naming the failing git command
- **AND** `premerge-assert.sh` subsequently refuses the merge for want of a certification

### Requirement: A merge-result summary is structurally distinguishable from a branch-head summary

The merge-result summary SHALL carry its **own marker pair**, its **own `MODE:` line**, and keys naming
the `origin/main` tip it composed against, the branch head it composed in, and the composed commit.
Neither block SHALL be usable as the other, and that SHALL hold **structurally**, not by label alone:

- a merge-result block SHALL fail a branch-head assertion because its `commit:`/`tree-start:` name the
  composed commit, which can never equal the certified PR head;
- a branch-head block SHALL fail a merge-result assertion because the merge-result marker and `MODE:`
  line are required **affirmatively**, as Case B already requires `MODE: delta`.

`scripts/gate-liveness.sh` SHALL recognise the new dialect; its marker regex is a closed three-dialect set
today, and an unrecognised fourth dialect makes a running merge-result gate unreadable to the liveness
reader (#3473).

#### Scenario: A merge-result block cannot be pasted as a gate of record
- **GIVEN** a merge-result summary with `RESULT: PASS`
- **WHEN** it is supplied as `premerge-assert.sh`'s full-gate summary argument
- **THEN** the assert refuses it by name
- **AND** the refusal does not depend on the marker alone

#### Scenario: A branch-head block cannot be pasted as a merge-result certification
- **GIVEN** a full `AGENT-GATE SUMMARY` with `RESULT: PASS`
- **WHEN** it is supplied where a merge-result certification is required
- **THEN** the assert refuses it, naming the absent merge-result mode line

#### Scenario: The liveness reader recognises a merge-result run
- **GIVEN** a merge-result gate writing a summary file
- **WHEN** `scripts/gate-liveness.sh` is run against that summary path
- **THEN** it reports `RUNNING` or `COMPLETE` rather than failing to parse the dialect

### Requirement: A merge-result certification is itself subject to the same staleness predicate

A merge-result certification composed against an `origin/main` that has **since** moved SHALL be treated
as stale by the **same predicate** applied to the composed base, so that the freshness question does not
recurse into a second definition. Because the composed commit carries the `origin/main` tip of the
composition as a parent, `merge-base(origin/main, <composed commit>)` **is** that composed base, and the
existing `scripts/flow/base-staleness.sh` answers the question **unmodified**, with the composed commit as
its subject.

This property is conditioned on the branch head **not being an ancestor of `origin/main`**, which holds
for any unmerged PR. That condition SHALL be pinned by a test, because a fixture violating it collapses
the merge-base onto the branch head and reports `diff-paths 0` — a case that passes for the wrong reason.

#### Scenario: A merge-result certification composed against a superseded main is stale
- **GIVEN** a merge-result certification composed against an `origin/main` tip that is now behind
- **AND** at least one commit landed since that tip touching the diff's blast radius
- **WHEN** the advisory is run with the composed commit as its subject
- **THEN** its `base` line names the composed base
- **AND** its verdict is `STALE-RECOGNISED` and it exits `4`

#### Scenario: The recursion terminates rather than regressing
- **GIVEN** a merge-result certification re-taken after a stale verdict
- **WHEN** the predicate is applied again
- **THEN** it is the same predicate over a strictly more recent composed base

#### Scenario: An on-main branch head is rejected as a fixture, not measured as fresh
- **GIVEN** a composed commit whose branch-head parent is an ancestor of `origin/main`
- **WHEN** the pinning test runs
- **THEN** the test recognises the degenerate fixture rather than recording its `diff-paths 0` as a pass

### Requirement: A stale certification requires merge-result certification before merge

When the base-staleness advisory reports `STALE-RECOGNISED`, `premerge-assert.sh` SHALL require a
merge-result certification for the PR and SHALL **refuse the merge** when none is supplied or when the one
supplied does not carry `RESULT: PASS`, `tree-integrity: PASS` and `dirty: no`. When the advisory reports
`NO-STALENESS-RECOGNISED`, a merge-result certification SHALL NOT be required — the owner's blast-radius
ruling is what keeps hot-repo lanes out of a re-gate loop.

The advisory's verdict SHALL be matched **token-exactly** against a closed set. A prefix test SHALL NOT be
used: `PASS*` accepts `PASSthisNeverRan`, which checks a spelling rather than a state.

#### Scenario: A stale PR with no merge-result certification is refused
- **GIVEN** a PR whose advisory reports `STALE-RECOGNISED`
- **AND** a valid full-gate certification at the PR head
- **WHEN** `premerge-assert.sh` is run without a merge-result certification
- **THEN** it refuses the merge with a cause naming the missing certification
- **AND** it exits non-zero

#### Scenario: A fresh-based PR is unaffected
- **GIVEN** a PR whose advisory reports `NO-STALENESS-RECOGNISED`
- **WHEN** `premerge-assert.sh` is run with only a head certification
- **THEN** it does not require a merge-result certification

### Requirement: Every non-affirmative advisory state is treated as stale

`premerge-assert.sh` SHALL treat advisory exit `5` / `UNMEASURED` as **stale**, and SHALL likewise treat a
usage error, an unrecognised verdict token, an empty output and a failure to run the advisory at all as
stale. A pass SHALL NOT be derived from the absence of a bad signal.

#### Scenario: UNMEASURED requires certification exactly as STALE-RECOGNISED does
- **GIVEN** an advisory that exits `5` with verdict `UNMEASURED`
- **WHEN** `premerge-assert.sh` is run without a merge-result certification
- **THEN** it refuses the merge
- **AND** a test asserting this reds if `5` is treated as fresh

#### Scenario: An unrecognised verdict token is stale
- **GIVEN** an advisory emitting a verdict token outside the closed set
- **WHEN** `premerge-assert.sh` evaluates it
- **THEN** it treats the run as stale and refuses

### Requirement: A merge result that fails is refused even when the head certification passes

A PR whose head certification legitimately PASSes but whose **merge result** FAILS SHALL be refused. This
is the case the whole change exists for: the head assert accepts a PASS at a stale head, and the merge
composes two things never tested together.

#### Scenario: A genuine head PASS with a failing merge result is refused
- **GIVEN** a stale-based PR with a valid full-gate PASS at its head
- **AND** a merge-result certification reporting `RESULT: FAIL`
- **WHEN** `premerge-assert.sh` is run
- **THEN** it refuses the merge
- **AND** the test covering this reds if the requirement is removed

### Requirement: The enforcement is mutation-checked

Each new fail-closed leg SHALL be covered by a test that **reds when the leg is removed or inverted**, per
#3465's and slice 1's precedent. `scripts/tests/test_premerge_assert.sh` SHALL additionally gain a **case
floor**: it has none today, so a span-replacing edit can delete cases and still report `failed: 0` —
the hazard #3544 records for its own suite, which lost four cases for a whole review round.

#### Scenario: Removing the enforcement reds the suite
- **GIVEN** the enforcement leg removed from `premerge-assert.sh`
- **WHEN** `scripts/tests/test_premerge_assert.sh` runs
- **THEN** at least one case fails

#### Scenario: Deleting cases reds the suite
- **GIVEN** a case removed from `scripts/tests/test_premerge_assert.sh`
- **WHEN** the suite runs
- **THEN** the case floor fails the suite rather than reporting `failed: 0`

### Requirement: Doctrine states what a `PREMERGE: OK` now proves

The #3465 scope disclaimer SHALL be updated in all five locations — `scripts/flow/premerge-assert.sh`
(header residual 3 and the success output), `CLAUDE.md`, `.claude/agents/flow-closer.md` and
`.claude/skills/flow-address/SKILL.md` — to state what a `PREMERGE: OK` proves **after** this change, and
`scripts/tests/test_premerge_assert.sh` Case 39, which pins the retained literals, SHALL be extended in
the same diff.

Doctrine SHALL NOT claim more than the mechanism delivers. It SHALL state that the blast radius remains
**non-exhaustive** — slice 1's declared gap 1 of 2, the dependency closure — and that enforcement makes
that gap *blocking* rather than closed. It SHALL state which half of the demonstration can run on this
PR (`premerge-assert.sh` is read from the checkout) and which cannot (a `required`-registry or aggregator
change is read from the PR's base ref, #2910).

#### Scenario: The disclaimer is retired without overclaiming
- **GIVEN** the doctrine text after this change
- **WHEN** a reader looks for what a `PREMERGE: OK` proves
- **THEN** it states the merge-result gap is closed for stale-based PRs
- **AND** it still declares the blast radius non-exhaustive
- **AND** no location still names the merge-result gate as unimplemented
