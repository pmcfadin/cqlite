# Spec delta: review-stage verdict (issue #3751)

## ADDED Requirements

### Requirement: A delegated review stage's verdict is an artifact, pre-stamped at spawn

The system SHALL provide `scripts/flow/review-stage.sh open <kind> --issue <N> --agent <type>` which
creates the stage's report-of-record file **before** the agent is spawned, pre-stamped with a
non-verdict sentinel recording `spawned-at`, `agent`, `issue`, and a `deadline`.

#### Scenario: silence is a readable state, not an absence
- **WHEN** a stage is opened and the spawned agent writes nothing
- **THEN** the report file EXISTS and its recorded result is the sentinel, not an empty or missing file

#### Scenario: the report path is verified gitignored, fail-closed
- **WHEN** `open` resolves a report path that `git check-ignore` does not confirm as ignored
- **THEN** it REFUSES to write, naming the path and the reason
- **AND** the refusal is exit-nonzero, so a mid-gate tree mutation (#2926) cannot be caused by this tool

#### Scenario: re-opening does not silently reset the clock
- **WHEN** `open` is called for a stage that is already open, without `--force`
- **THEN** it REFUSES, and the original `spawned-at` is preserved

### Requirement: An absent report is reported as NOT-RUN and is non-passing

`review-stage.sh verdict <kind> --issue <N>` SHALL emit exactly one line of a CLOSED grammar:
`REVIEW-STAGE: <kind> RESULT: <token> elapsed=<secs> deadline=<secs> agent=<type> report=<path>`
with `<token>` from `{PASS, FINDINGS, NOT-RUN, AUTHOR-PERFORMED}`, matched by string equality on the
first word, and with any unrecognised, empty, sentinel-only, ungrammatical, absent, or never-opened
state reported as `NOT-RUN`. The recorded result SHALL be read from a `result:` line ANCHORED AT
COLUMN ZERO: the report body is author-controlled text that carries example verdict lines BY DESIGN
(the pre-stamped sentinel has to show the agent the spelling, and a review report quotes other
reports), so an indented, quoted or bulleted copy is DATA and not the record.

#### Scenario: the result line is read at column zero only
- **WHEN** the report's only `result:` line is indented, quoted or bulleted
- **THEN** the verdict is `NOT-RUN`, naming the absent result line — never the value that copy carries
- **AND** an ordinary column-zero `result:` line is still read (the anchor is not a refusal of all input)

#### Scenario: a stage that produced nothing is never clean and never empty-findings
- **WHEN** the stage's report is sentinel-only
- **THEN** the verdict token is `NOT-RUN` and the exit status is non-zero
- **AND** the token is neither `PASS` nor any value a consumer may read as a passing verdict

#### Scenario: each NOT-RUN cause is named
- **WHEN** the report is absent / empty / ungrammatical / the stage was never opened
- **THEN** the `NOT-RUN` token carries a parenthesised cause distinguishing that state from the others

#### Scenario: an unrecognised token is not passed through
- **WHEN** a report records a result token outside the closed set
- **THEN** the verdict is `NOT-RUN (report ungrammatical: …)`, never the unrecognised token

### Requirement: The deadline is visible and advisory

`review-stage.sh status <kind> --issue <N>` SHALL report elapsed time, the deadline, and whether the
report is still sentinel-only; past the deadline it SHALL state the elapsed time and the fact that
nothing was produced. The deadline SHALL NOT change the verdict.

#### Scenario: a stage past its deadline does not look hung
- **WHEN** a stage is sentinel-only past its deadline
- **THEN** `status` names the elapsed time and that no report was produced

#### Scenario: a late report is still a report
- **WHEN** a real report is written after the deadline
- **THEN** the verdict is derived from its content, not from the deadline

### Requirement: The merge point fails closed on a missing C verdict

`scripts/flow/premerge-assert.sh` SHALL accept `--c-verdict <path|AUTO>`, SHALL treat its absence as a
usage failure (exit 3), and SHALL determine whether C is required by MEASURING the branch — an
`openspec/changes/<slug>/` present ⇒ C required; absent ⇒ C not applicable, reported affirmatively.

#### Scenario: an absent C verdict cannot reach a merge on a design-routed branch
- **WHEN** the branch carries an OpenSpec change and the C verdict is absent or `NOT-RUN`
- **THEN** `premerge-assert.sh` REFUSES, naming the stage and the cause

#### Scenario: routing is measured, not asserted by the caller
- **WHEN** no OpenSpec change exists on the branch
- **THEN** the script reports `c-verdict: NOT-APPLICABLE (no openspec change on branch)` affirmatively
- **AND** no caller-supplied flag value can declare C inapplicable on a branch that carries one

#### Scenario: a missing flag is loud
- **WHEN** `--c-verdict` is omitted entirely
- **THEN** the script exits 3 (usage) rather than defaulting to "not required"

#### Scenario: a symlinked write path is refused, not followed
- **WHEN** the report path, the stage-record path, or any component under `.review-stage/` is a SYMLINK
- **THEN** `open` (and `record-author-performed`) REFUSE naming the symlink and the component, the link
  target is left UNTOUCHED, and the ordinary non-symlinked path still succeeds (the positive control)

#### Scenario: archiving a completed change is not design-routed
- **WHEN** the branch's only change under `openspec/changes/` is a real move of a live change
  directory into `openspec/changes/archive/`
- **THEN** the routing measure reports `NOT-APPLICABLE (no openspec change on branch)` and the merge
  proceeds — a deletion is not a routing signal, and refusing here would be a false refusal on
  doctrine-mandated input
- **AND** an ADDED or MODIFIED path under a live `openspec/changes/<slug>/` still routes to C

#### Scenario: an AUTO-located stage is bound to the certified tree
- **WHEN** `--c-verdict AUTO` locates a stage in a worktree whose `HEAD` is not the certified commit
- **THEN** `premerge-assert.sh` REFUSES, naming the divergence — every lane is a worktree of one shared
  `.git`, so a peer lane's certified commit resolves from any lane and resolvability is not provenance
- **AND** the same stage at the worktree's own `HEAD` certifies (the positive control)

#### Scenario: a sibling stage's PASS cannot certify C
- **WHEN** the verdict line names a stage kind other than `c`, or omits any of
  `elapsed=`/`deadline=`/`agent=`/`report=`, or carries one of them twice
- **THEN** `premerge-assert.sh` REFUSES as ungrammatical, naming what was wrong — the stage kind is
  compared by STRING EQUALITY and each mandatory key must appear EXACTLY ONCE

### Requirement: A hand-performed substitute is recorded as author-performed, never as clean

`review-stage.sh record-author-performed` SHALL require a substantive `--reason`, a named `--evidence`
artifact and `--performed-by author|peer`, SHALL refuse placeholder values, and SHALL cause `verdict` to
report the DISTINCT token `AUTHOR-PERFORMED` — never `PASS`. The recorded disclosure SHALL carry the
form: *"an author's hand audit is not an independent one; weight it accordingly."* It SHALL NOT
replace a report that already RECORDS a verdict (`PASS` or `FINDINGS`) unless `--force` is passed, and
a forced replacement SHALL record the replaced token in the new report.

#### Scenario: a recorded verdict is not silently replaced
- **WHEN** the stage's report already records `FINDINGS` and a substitute is recorded without `--force`
- **THEN** the recording is REFUSED, naming the recorded token, and the report is left intact
- **AND** with `--force` the new report NAMES the token it replaced, so the substitution is auditable
- **AND** a sentinel-only report is replaced with no `--force` (the normal path is unaffected)

#### Scenario: a substitute is distinguishable from an independent audit
- **WHEN** an author-performed C is recorded and the verdict is read
- **THEN** the token is `AUTHOR-PERFORMED` and a reader grepping the passing token does not match it

#### Scenario: an unfilled template is not a disclosure
- **WHEN** `--reason` is a placeholder (`why`/`todo`/`tbd`) or carries an unsubstituted `<…>`
- **THEN** the recording is REFUSED as a usage error

#### Scenario: the classifier is as strong as the writer
- **WHEN** a HAND-WRITTEN report asserts `result: AUTHOR-PERFORMED` with the disclosure but with a
  `performed-by` outside `author|peer`, or a `reason`/`evidence` the writer would refuse as a
  placeholder, too short or an unsubstituted `<…>`
- **THEN** `verdict` reports `NOT-RUN (report ungrammatical: AUTHOR-PERFORMED …)`, naming the field and
  the defect — never `AUTHOR-PERFORMED`
- **AND** a hand-written report WITH real working still reports `AUTHOR-PERFORMED` (the positive control)

### Requirement: The mechanism is recorded, with its limits

The change SHALL commit a root-cause record stating, from committed source, that no agent in
`.claude/agents/` has `SendMessage`, that the Agent tool's terminal result is therefore the only channel,
and that naming a report path is NOT a fix for the agents (measured: effective for `spec-auditor` and
`flow-closer`, 0/3 for `rust-reviewer`).

#### Scenario: the claim made is the narrow one
- **WHEN** the record is read
- **THEN** it claims a correct CONSUMING verdict, and does not claim that flaky agents now deliver

### Requirement: The guard cannot green vacuously

`scripts/tests/test_review_stage.sh` SHALL be enrolled in the `tooling-tests` roster and SHALL contain a
positive control (a real report ⇒ `PASS`, exit 0, merge assert proceeds) alongside the absent-report case,
plus a case floor.

#### Scenario: an always-NOT-RUN implementation fails the suite
- **WHEN** the mechanism reports `NOT-RUN` for every input
- **THEN** the positive control FAILS

#### Scenario: a shrunken suite is caught
- **WHEN** the suite's executed case count falls below the committed floor
- **THEN** the suite FAILS rather than reporting zero failures
