# cli-graceful-shutdown-test-oracle — delta for cli-tests-sigint-flake (issue #3515)

**Scope.** `cqlite-cli/tests/graceful_shutdown_tests.rs` — both tests in the file. Test code only; no
`src/` change. The subject property is unchanged (SIGINT in a writable session flushes durably and
exits cleanly); what changes is the **oracle** used to observe it.

**Acceptance-criterion → requirement map** (issue #3515):

| AC | Requirement(s) |
|---|---|
| AC1 — no longer fails on a contended host while the handler works; prefer a property-observing oracle with liveness confirmation | ADDED *The shutdown oracle confirms the handler was entered before attributing a timeout*; ADDED *The test is bounded by ONE deadline, calibrated from in-band measurements taken on the same host*; ADDED *The exit wait observes and reports progress* |
| AC2 — the failure message must not assert a cause the measurement cannot establish | ADDED *Every wait failure reports only what its measurement establishes* |
| AC3 — RED-verify by actually breaking the shutdown handler | ADDED *The new oracle is observed to red on a genuinely broken handler* |
| AC4 — check the sibling test for the same shape | ADDED *The sibling threshold-flush test carries the same oracle* |
| — (design obligation, `design.md`) | ADDED *The test is bounded by ONE deadline, calibrated from in-band measurements taken on the same host* (round 7: renamed from *below the harness hard-kill*, a premise verified FALSE; round 8: the per-stage budget layer DESCOPED into this single requirement — see `design.md` D6a) |

**Round-8 DESCOPE (`design.md` D6a).** roborev returned **12 findings across four rounds, all 12 in
the per-stage calibrated budget layer**, at a flat 3 per round, while the oracle proper produced none
after round 3. That layer is therefore replaced by **ONE deadline per test**, and the requirements
below are rewritten accordingly: three requirements are MERGED into *The test is bounded by ONE
deadline…* and each withdrawn obligation is named where it stood, never silently dropped. The ACs
never asked for per-stage calibration — AC1's liveness confirmation is supplied by the handler-entry
marker.

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
- **WHEN** the handler-entry marker is observed and the child then fails to exit before the deadline
- **THEN** the failure SHALL state that the shutdown flush did not complete before the deadline
- **AND** the failure SHALL NOT state or imply that a shutdown handler is missing, absent, or unimplemented

#### Scenario: handler entry never observed
- **WHEN** no handler-entry marker is observed before the deadline
- **THEN** the failure SHALL name the exact substring it awaited
- **AND** SHALL print the transcript of what the child actually emitted
- **AND** SHALL name signal non-delivery, handler non-entry, and product marker-text drift as the candidate causes, without selecting between them

### Requirement: The test is bounded by ONE deadline, calibrated from in-band measurements taken on the same host

Each test SHALL be bounded by exactly **one** deadline. That deadline SHALL be
`clamp(base × scale, base, cap)`, where `scale = max(1, observed / quiet_baseline)` is taken over the
in-band measurements of this same run on this same host — the spawn → readiness-banner duration
(`t_boot`) and the write → `OK` round-trip (`t_ack`) — and the LARGEST such scale SHALL be the one
used.

Calibration SHALL only ever LOOSEN the deadline, never tighten it. That property SHALL come from the
formula — `scale` floored at 1, the span clamped at `base`, and the largest scale retained — and SHALL
NOT be obtained by making `quiet_baseline` large.

`quiet_baseline` SHALL sit above every recorded quiet measurement and below every measurement recorded
under real contention, and BOTH directions SHALL be asserted by a unit test against those recorded
numbers. A baseline far above the quiet noise floor makes the mechanism INERT: measured, the first
version's 500ms/200ms baselines left `scale` at exactly 1.000 in every run including load average 116.

There SHALL be no per-stage budget, no per-stage cap, and no arithmetic that composes stage allowances.
**Any single stage SHALL be able to consume the whole deadline.**

*(Round-8 withdrawal, recorded rather than deleted: this requirement previously demanded a per-stage
`base`/`cap` pair per wait, a `quiet_baseline` "within a small single-digit multiple" of a committed
per-observation ANCHOR, and a NOTICE when a host measured below its anchor. All three are withdrawn.
The anchor/multiple factoring made the anchor the sole source of truth and therefore unverifiable —
planting a permissive anchor scaled the baseline with it and every assert still passed — and the
mechanism it protected is gone. The surviving guard asserts the baseline against BOTH a recorded quiet
and a recorded loaded measurement, which is what "not inert" actually means.)*

#### Scenario: quiet host
- **WHEN** every in-band measurement is under `quiet_baseline`
- **THEN** the deadline SHALL equal `base`

#### Scenario: contended host
- **WHEN** an in-band measurement is inflated by host contention
- **THEN** the deadline SHALL be inflated in proportion, up to `cap`

#### Scenario: a later, faster measurement
- **WHEN** a measurement yielding a smaller scale is folded in after a larger one
- **THEN** the deadline SHALL NOT move earlier

#### Scenario: a baseline inflated away from its measurements
- **WHEN** `quiet_baseline` is raised past the slowest recorded QUIET measurement, or above the fastest recorded LOADED one
- **THEN** a unit test SHALL fail, naming the baseline, the measurement, and that the calibration would be inert (or would scale on an unloaded host)

#### Scenario: deadline derivation is reported
- **WHEN** any wait fails
- **THEN** the failure SHALL report the deadline, the `base`, the `scale`, the `cap`, and every measured duration the `scale` was taken over

### Requirement: No wait is tighter than the bound it replaced

No wait in the file SHALL be able to fire sooner than the wall-clock bound it replaced, and the whole
test SHALL NOT be bounded more tightly than the nominal aggregate of the bounds it replaced.

Because any single stage may consume the whole deadline, this reduces to two properties of the
deadline's `base`, both of which SHALL be asserted by a unit test:

* `base` ≥ the old per-wait bound (60s), so no single wait is tighter; and
* `base` ≥ the sum of the nominal bounds the test replaced **plus one old per-wait bound for every
  stage that now draws on the deadline but was not separately bounded before** (the readiness stage:
  the old code folded boot inside its first 60s `OK` wait), so the test as a whole is not tighter even
  when that stage consumes a full old bound first.

*(Round-9 correction, roborev job 232 finding 2: the aggregate term previously summed only the OLD
waits, which admitted a base under which readiness consumes 60s and leaves every original wait below
its former allowance — the invariant violated by a stage the sum did not mention.)*

*(Round-8 withdrawal, recorded rather than deleted: this requirement previously stated the invariant
**by composition** — a mapping from each old bound to the GROUP of new stages that replaced it, whose
bases had to sum to at least the old value — together with a GROUP DEADLINE for repeated operations
and a scenario requiring a clipped stage to name its own starvation. The composition rule was wrong
twice: it was set below the old bound in round 3, and roborev job 229 found that summing per-stage caps
does not preserve a SHARED old deadline, so a handler entering at 31s and exiting at 32s — which the
old flat 60s allowed — failed a 30s per-stage cap. With one deadline there is nothing to compose, no
group to deadline and no stage to starve, so the invariant holds unconditionally and trivially, which
is strictly stronger than the formulation it replaces.)*

This invariant SHALL be asserted by a unit test, not merely documented — a comment cannot fail.

#### Scenario: a deadline tightened below the bound it replaced
- **WHEN** the deadline's `base` is reduced below the old per-wait bound or below the aggregate it replaced
- **THEN** a unit test SHALL fail, naming the base and the bound

#### Scenario: one slow operation among fast siblings
- **WHEN** repeated operations run under the one deadline and all but one complete quickly
- **THEN** the remaining operation SHALL be able to consume the whole remaining deadline, which exceeds the full old bound

#### Scenario: a later stage after slow earlier ones
- **WHEN** earlier stages legitimately consume time while the product behaves correctly
- **THEN** no allowance SHALL have been deducted from any later stage, because no stage has an allowance

**The irreducible bound, named rather than left to be rediscovered.** The deadline's `base` applies
before any measurement exists, so the first stage of each test — the readiness-banner wait — runs under
an UNCALIBRATED bound. Calibrating it would require a measurement taken before the test began, whose
own bound would need a measurement before *that*. It is exempt rather than silently non-compliant, and
the failure message SHALL say so.

### Requirement: The exit wait observes and reports progress

The post-`SIGINT` wait for process exit SHALL NOT be a single opaque `wait_timeout` call. It SHALL poll
in slices and SHALL OBSERVE each of the following:

* a newly observed line on the child's `stderr` or `stdout`;
* an increase in the count of durable `-Data.db` artifacts under the write directory.

Observed progress SHALL be reported as EVIDENCE in any failure message — including an explicit
`progress observed: NONE` with zero counts when nothing was seen — and SHALL NOT extend, reset or
otherwise alter any bound.

The deadline SHALL be checked BEFORE each poll step is invoked, and each step SHALL be given no more
than the time remaining, so that no wait is ever STARTED past the deadline and none is granted more
than what it leaves.

**The deadline bounds WAITING FOR EVIDENCE, not the acceptance of evidence already observed.** A poll
step that observes its success — the child exited, or a durable artifact appeared — SHALL return that
success even if the deadline lapsed while it was looking, and the harness SHALL NOT recheck the
deadline on the success path. Rejecting an observed success because it was noticed late would be a
false failure on a working product, which is the flake class this change exists to remove, and it would
make the verdict depend on how long a directory scan took. The lag between the deadline and the
returned verdict SHALL be bounded — at most one poll slice plus one artifact scan — and that bound, and
the fact that an artifact scan is a recursive directory walk that is not necessarily quick on a loaded
host, SHALL be stated where the claim is made.

*(Round-9 ruling, roborev job 232 finding 1: the review proposed rechecking the deadline before
returning success. It was OVERRULED — the behaviour is correct and the CLAIM was overstated. See
`tasks.md` round 9.)*

*(Round-8 withdrawal, recorded rather than deleted: progress previously RESET a calibrated stall window
and extended the stage past its nominal budget. That is precisely what made a declared cap not the
actual maximum — the defect family four review rounds could not close — so the crediting is withdrawn
and only the observation survives. What AC1 asks for, liveness confirmation, comes from the
handler-entry marker and from these reported counts, neither of which is a bound.)*

#### Scenario: flush is landing slowly
- **WHEN** the child has not exited but durable artifacts are still appearing
- **THEN** the wait SHALL continue until the deadline, and any failure SHALL report the artifacts it saw

#### Scenario: continuous progress up to the deadline
- **WHEN** progress arrives on every poll slice
- **THEN** the wait SHALL still end at the deadline, and a unit test SHALL assert that it terminates

#### Scenario: no progress at all
- **WHEN** the child has neither exited nor produced any new output or artifact and the deadline passes
- **THEN** the wait SHALL fail with the attribution required above, reporting `progress observed: NONE`

### Requirement: Every wait failure reports only what its measurement establishes

No failure message in this file SHALL assert a cause that its own measurement cannot establish. The
string `no graceful shutdown handler` SHALL NOT appear. Each such message SHALL name what was awaited,
the deadline and its derivation, what was observed (including the child transcript), and — where the
measurement is genuinely ambiguous — the candidate causes without selecting one.

#### Scenario: no unestablishable cause survives in the file
- **WHEN** the file's wait-failure messages are read
- **THEN** none SHALL claim a missing handler, a dead-ended session, or an unused code path as an established fact on the strength of a timeout alone

### Requirement: The sibling threshold-flush test carries the same oracle

`writable_session_auto_flushes_mid_session_across_threshold` SHALL carry the same oracle: attribution
stages under its own ONE deadline, with progress observed and reported on the stages that poll.
Specifically its per-write acknowledgement wait, its mid-session durable-artifact wait, and its
stdin-EOF exit wait SHALL each be bounded by that one deadline and SHALL each report only
what they measure — replacing the present claims that the session "dead-ended" or that the interactive
loop "did not use the threshold-flushing path", neither of which a timeout establishes.

#### Scenario: sibling under contention
- **WHEN** the host is contended and the threshold-flushing path is working
- **THEN** the sibling test SHALL NOT fail on any of its three waits

### Requirement: The one deadline is the only bound, and there is one place a per-wait timeout is computed

Each test SHALL track its elapsed time against its one deadline and SHALL emit its own attributed
failure on reaching it, naming the stage that was pending. Stages exist for that ATTRIBUTION and for
nothing else: a stage SHALL carry a name and a start instant and SHALL NOT carry a bound.

Every wait — the line waits, the child `wait_timeout`s, the pipe-collection `recv_timeout`s and the
progress-observing poll — SHALL take its timeout from that one deadline, through exactly one method.
No call site SHALL subtract elapsed time and no call site SHALL be handed a fresh allowance.

**This requirement previously required that budget to sit "below nextest's configured hard kill
(240s)". That premise was FALSE for this test and is withdrawn** — `cli-tests` runs plain
`cargo test`, and nothing in the gate or CI runs `cqlite-cli` under nextest, so no harness timeout
applies (see `design.md` D6). Because no harness bound exists, this self-imposed deadline is the ONLY
bound on a wedged run, and it SHALL therefore still exist and SHALL be bounded above so that it cannot
outlast the gate component it runs in.

*(Round-8 withdrawal, recorded rather than deleted: this requirement previously also demanded that the
SUM of every stage's declared maximum fit inside the total, that a stage's declared maximum include its
progress extension, and that no stage be STARVED by an earlier one. All three are withdrawn because
they are unstatable now, not because they stopped mattering: there is no per-stage maximum to sum, no
extension to count, and no allowance an earlier stage could consume — which is the same guarantee
those three clauses were trying to buy, obtained by construction instead of by arithmetic.)*

#### Scenario: two waits in one stage
- **WHEN** a stage performs more than one bounded wait, with work between them
- **THEN** that work SHALL be charged to the deadline, and the second wait SHALL NOT be GRANTED more than the deadline less what is already spent

#### Scenario: a slow but working host
- **WHEN** several stages legitimately run slowly while the product behaves correctly
- **THEN** no later stage SHALL be reduced, because no stage has an allowance to reduce

#### Scenario: everything is slow
- **WHEN** the deadline is reached
- **THEN** the test SHALL fail with its own attributed message naming the stage that was pending, its own duration, and how the deadline was derived

#### Scenario: the deadline cannot outlast its gate component
- **WHEN** the deadline's `cap` is raised
- **THEN** a unit test SHALL fail if it exceeds the recorded limit, because a self-termination that outlasts the run it protects protects nothing

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
- **AND** it SHALL do so at the test's one deadline. **This is the round-8 descope's accepted cost, stated as a requirement so it cannot be mistaken for a regression**: a genuine defect no longer fails fast against a tight per-stage cap. It is paid only on a real failure, and it buys the elimination of the defect family four review rounds could not close.
