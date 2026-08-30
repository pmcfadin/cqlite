# cli-graceful-shutdown-test-oracle — delta for cli-tests-sigint-flake (issue #3515)

**Scope.** `cqlite-cli/tests/graceful_shutdown_tests.rs` — both tests in the file. Test code only; no
`src/` change. The subject property is unchanged (SIGINT in a writable session flushes durably and
exits cleanly); what changes is the **oracle** used to observe it.

**Acceptance-criterion → requirement map** (issue #3515):

| AC | Requirement(s) |
|---|---|
| AC1 — no longer fails on a contended host while the handler works; prefer a property-observing oracle with liveness confirmation | ADDED *The shutdown oracle confirms the handler was entered before attributing a timeout*; ADDED *Wall-clock ceilings are calibrated from in-band measurements taken on the same host*; ADDED *The exit wait is progress-checked* |
| AC2 — the failure message must not assert a cause the measurement cannot establish | ADDED *Every wait failure reports only what its measurement establishes* |
| AC3 — RED-verify by actually breaking the shutdown handler | ADDED *The new oracle is observed to red on a genuinely broken handler* |
| AC4 — check the sibling test for the same shape | ADDED *The sibling threshold-flush test carries the same oracle* |
| — (design obligation, `design.md`) | ADDED *The test owns a total budget that every stage's allowance fits inside* (round 7: renamed from *below the harness hard-kill*, a premise verified FALSE — see the requirement) |

## ADDED Requirements

### Requirement: The shutdown oracle confirms the handler was entered before attributing a timeout

After delivering `SIGINT`, the test SHALL wait for the child's own **handler-entry progress marker**
on `stderr` (the `Received Ctrl-C — flushing memtable before exit` text emitted by
`cqlite-cli/src/main.rs`) **before** it waits for process exit, and SHALL treat observation of that
marker as establishing that the signal was delivered, that a shutdown handler exists and was entered,
and that the child was scheduled.

The test SHALL NOT attribute a subsequent exit timeout to a missing or absent handler once that marker
has been observed.

`stderr` SHALL be drained by a reader for the lifetime of the child, so that the marker is observable
and so that an undrained pipe cannot wedge the child.

#### Scenario: handler entry observed, exit slow
- **WHEN** the handler-entry marker is observed and the child then fails to exit within the exit budget
- **THEN** the failure SHALL state that the shutdown flush did not complete within the budget
- **AND** the failure SHALL NOT state or imply that a shutdown handler is missing, absent, or unimplemented

#### Scenario: handler entry never observed
- **WHEN** no handler-entry marker is observed within its budget
- **THEN** the failure SHALL name the exact substring it awaited
- **AND** SHALL print the transcript of what the child actually emitted
- **AND** SHALL name signal non-delivery, handler non-entry, and product marker-text drift as the candidate causes, without selecting between them

### Requirement: Wall-clock ceilings are calibrated from in-band measurements taken on the same host

Every wait budget in the file that follows a completed measurement SHALL be derived as
`clamp(base × scale, base, cap)`, where `scale = max(1, observed / quiet_baseline)` and `observed` is a
duration **measured during this same test run on this same host**:

* the spawn → readiness-banner duration (`t_boot`) SHALL calibrate the write-acknowledgement budget;
* the write → `OK` round-trip (`t_ack`) SHALL calibrate the handler-entry and exit budgets.

Calibration SHALL only ever LOOSEN a budget, never tighten one. That property SHALL come from the
formula — `scale` floored at 1 and `derived` clamped at `base` — and SHALL NOT be obtained by making
`quiet_baseline` large.

`quiet_baseline` SHALL sit just above the recorded measured quiet value for its observation, within a
small single-digit multiple of it. **An earlier version of this requirement demanded the opposite** (a
baseline "large enough that an unloaded host yields `scale == 1`"), and it was measured to make the
mechanism INERT — `scale` stayed at exactly 1.000 in every run including load average 116. Since
calibration cannot tighten a budget, an over-eager `scale` cannot fail a test, so there is no
quiet-side risk that a large baseline buys; under-engagement is the only hazard. See `design.md` D2.

Each `quiet_baseline` SHALL be anchored to a committed recorded measurement and asserted against it by
a unit test, both from below (at or above the measurement) and from above (within the stated multiple).
Where one baseline governs observations from more than one test, the anchor SHALL be the measurement
that BINDS — the smallest relevant quiet value — because the anchor forms the basis of an upper bound on
the baseline, in which direction "slowest observed" is the permissive choice.

#### Scenario: a baseline inflated away from its measurement
- **WHEN** a `quiet_baseline` is raised beyond the stated multiple of its anchoring measurement
- **THEN** a unit test SHALL fail, naming the baseline, the measurement, and that the calibration would be inert

### Requirement: No wait is tighter than the bound it replaced

For each wall-clock bound present before this change, the GROUP of new stages that replaced it SHALL
be able to consume at least that old bound. The invariant is **by composition**: a single old bound was
often split across several new stages, and each new stage can look innocent while its group is tighter.

Where repeated or numerous operations previously held INDEPENDENT bounds, each replacing stage SHALL
carry the full old bound as its own allowance. Any aggregate bound on such a group SHALL be a GROUP
DEADLINE, so that a single operation can still reach the full old bound when its siblings ran fast: a
reduction SHALL be contingent on the aggregate budget being genuinely consumed, and SHALL NOT be
imposed unconditionally by a small per-operation cap.

(An earlier version of this paragraph conditioned the group deadline on the group's nominal sum being
"not simultaneously realizable against the harness hard-kill". There is no harness hard-kill for this
test — see the total-budget requirement below — and the total budget is now sized so that every
group's nominal sum IS simultaneously realizable. The group deadline therefore remains as a BACKSTOP
on non-stage overruns rather than as the primary bound, and the per-operation floor above holds
unconditionally, which is the stronger of the two properties.)

This invariant SHALL be asserted by a unit test, not merely documented — a comment cannot fail.

#### Scenario: a stage tightened below its predecessor
- **WHEN** any stage's base is reduced so that its group can no longer reach the bound it replaced
- **THEN** a unit test SHALL fail, naming the group and the old bound

#### Scenario: one slow operation among fast siblings
- **WHEN** repeated operations share a group deadline and all but one complete quickly
- **THEN** the remaining operation SHALL be able to consume the full old bound

#### Scenario: the group budget is genuinely exhausted
- **WHEN** earlier stages have consumed the aggregate budget so a later stage is clipped to near zero
- **THEN** that stage's failure SHALL name the exhaustion as the cause, so it is distinguishable from the property not holding

**This requirement is deliberately NOT universal, and the exception is the point.** The first wait in
each test — for the readiness banner — has no prior measurement to calibrate against and SHALL remain a
bare wall-clock deadline. It is the irreducible bound identified in `design.md`; it covers only process
spawn and engine init, and it is exempt from this requirement rather than silently non-compliant with it.

#### Scenario: quiet host
- **WHEN** the calibrating measurement is well under `quiet_baseline`
- **THEN** the derived budget SHALL equal `base`

#### Scenario: contended host
- **WHEN** the calibrating measurement is inflated by host contention
- **THEN** the derived budget SHALL be inflated in proportion, up to `cap`

#### Scenario: budget derivation is reported
- **WHEN** any calibrated wait fails
- **THEN** the failure SHALL report the derived budget, the `base`, the `scale`, and the measured duration the `scale` came from

### Requirement: The exit wait is progress-checked

The post-`SIGINT` wait for process exit SHALL NOT be a single opaque `wait_timeout` call. It SHALL poll
in slices and SHALL treat each of the following as evidence of progress:

* a newly observed line on the child's `stderr` or `stdout`;
* an increase in the count of durable `-Data.db` artifacts under the write directory.

#### Scenario: flush is landing slowly
- **WHEN** the child has not exited but durable artifacts are still appearing
- **THEN** the wait SHALL continue rather than fail on a stall

#### Scenario: no progress at all
- **WHEN** the child has neither exited nor produced any new output or artifact for the stall window, and the total budget is exhausted
- **THEN** the wait SHALL fail with the attribution required above

### Requirement: Every wait failure reports only what its measurement establishes

No failure message in this file SHALL assert a cause that its own measurement cannot establish. The
string `no graceful shutdown handler` SHALL NOT appear. Each such message SHALL name what was awaited,
the budget and its derivation, what was observed (including the child transcript), and — where the
measurement is genuinely ambiguous — the candidate causes without selecting one.

#### Scenario: no unestablishable cause survives in the file
- **WHEN** the file's wait-failure messages are read
- **THEN** none SHALL claim a missing handler, a dead-ended session, or an unused code path as an established fact on the strength of a timeout alone

### Requirement: The sibling threshold-flush test carries the same oracle

`writable_session_auto_flushes_mid_session_across_threshold` SHALL use the same staged, calibrated,
progress-checked waits. Specifically its per-write acknowledgement wait, its mid-session
durable-artifact wait, and its stdin-EOF exit wait SHALL each be calibrated and SHALL each report only
what they measure — replacing the present claims that the session "dead-ended" or that the interactive
loop "did not use the threshold-flushing path", neither of which a timeout establishes.

#### Scenario: sibling under contention
- **WHEN** the host is contended and the threshold-flushing path is working
- **THEN** the sibling test SHALL NOT fail on any of its three waits

### Requirement: The test owns a total budget that every stage's allowance fits inside

Each test SHALL track its elapsed time across stages against a total budget, and SHALL emit its own
attributed failure on exhausting it.

**This requirement previously required that budget to sit "below nextest's configured hard kill
(240s)". That premise was FALSE for this test and is withdrawn** — `cli-tests` runs plain
`cargo test`, and nothing in the gate or CI runs `cqlite-cli` under nextest, so no harness timeout
applies (see `design.md` D6). Because no harness bound exists, this self-imposed budget is the ONLY
bound on a wedged run, and it SHALL therefore still exist.

The total budget SHALL be large enough that **the sum of every stage's declared maximum fits inside
it**, and that property SHALL be asserted. A stage's declared maximum SHALL include any legitimate
extension of that stage (notably the progress-checked poll's stall-window extension); an extension
that is not counted in the declared maximum makes the cap a number rather than a bound.

No stage SHALL be starved by an earlier stage's legitimate consumption. Starvation of a later stage
while the product is working is a FALSE failure of the same class this change exists to remove.

#### Scenario: every allowance fits
- **WHEN** the per-stage declared maxima are summed, including progress extensions
- **THEN** the sum SHALL be within the total budget, and a unit test SHALL assert it

#### Scenario: a slow but working host
- **WHEN** several stages legitimately run slowly while the product behaves correctly
- **THEN** no later stage SHALL be starved into failing

#### Scenario: everything is slow
- **WHEN** stages are slow enough that the total budget is reached
- **THEN** the test SHALL fail with its own attributed message naming the stage that consumed the budget

### Requirement: A stage's declared cap is its actual maximum, by construction

A stage SHALL own a single deadline computed once when its budget is derived, and every wait within
that stage SHALL derive its timeout from that deadline. There SHALL be exactly one place that computes
a per-wait timeout.

**Rationale, from four observed instances.** Where each wait site separately subtracts elapsed time,
one site always omits it: roborev found a stage exceeding its cap at four separate sites across two
review rounds (pipe collection given a fresh full allowance, the read-side spawn excluded from the
stage's own timing, and the progress extension omitted from the cap sum). Those are one defect, and
the per-site fix does not close it.

#### Scenario: two waits in one stage
- **WHEN** a stage performs more than one bounded wait
- **THEN** their combined elapsed time SHALL NOT exceed the stage's declared maximum

### Requirement: The new oracle is observed to red on a genuinely broken handler

The change SHALL be RED-verified by actually breaking the product shutdown path — not by a mocked or
simulated failure — and the observation recorded in the PR body. Two defects SHALL be exercised:

* the shutdown handler **removed** (so `SIGINT` takes its default disposition), and
* the shutdown flush **hung**.

For each, the test SHALL fail, and the reported cause SHALL be the true one.

#### Scenario: handler removed
- **WHEN** the `ctrl_c` branch of the interactive loop is removed and the test is run
- **THEN** the test SHALL fail, and SHALL do so at the handler-entry stage

#### Scenario: flush hung
- **WHEN** the shutdown flush is made to hang and the test is run
- **THEN** the test SHALL fail at the exit stage, reporting that the flush did not complete — not that a handler is missing
