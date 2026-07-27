# required-check-aggregation

## ADDED Requirements

### Requirement: `required` fails closed unless every registered gating tier has reported success

The `required` check in `.github/workflows/pr-gate.yml` SHALL, in addition to its own validation steps,
observe the sibling check runs on the pull request's head commit and SHALL report success ONLY when every
tier declared in the gating-tier registry has reported a terminal successful conclusion for that commit.

`required` SHALL fail when any registered tier is failed, timed out, cancelled without a superseding run,
still non-terminal at the aggregation deadline, or absent. Absence SHALL NEVER be interpreted as
inapplicability — inapplicability is communicated by the tier itself as an emitted success (see the
always-emit requirement), so an absent registered context is unconditionally an error.

The aggregation SHALL NOT mask the gate's own result: when the job carrying `pr-gate.yml`'s existing
validation steps does not conclude `success`, `required` SHALL fail regardless of tier state, and the
aggregating job SHALL still run and report (never be skipped) so the `required` context always exists.

Check runs that are NOT registered SHALL NOT gate the merge, so advisory lanes (perf, docs recipe smoke)
cannot block a PR by failing.

#### Scenario: All registered tiers succeeded
- **GIVEN** every context in the registry has a terminal check run on the PR head with conclusion `success`
  (or the tier's declared "not applicable" success form)
- **WHEN** `required` aggregates
- **THEN** `required` concludes success
- **AND** its job summary lists every registered context with the check-run id and conclusion it observed

#### Scenario: A registered tier failed
- **GIVEN** one registered context has a terminal check run with conclusion `failure`
- **WHEN** `required` aggregates
- **THEN** `required` FAILS with a non-zero exit and names the failing tier and its check-run URL

#### Scenario: A registered tier is absent
- **GIVEN** one registered context has NO check run on the PR head
- **WHEN** `required` aggregates and the deadline expires
- **THEN** `required` FAILS and names the absent tier
- **AND** the failure message states that absence is treated as an error, not as inapplicability

#### Scenario: The gate's own validation failed
- **GIVEN** the job carrying `pr-gate.yml`'s validation steps concluded `failure` or `cancelled`
- **AND** every registered tier succeeded
- **WHEN** the aggregating job runs
- **THEN** it still runs (it is not skipped) and `required` FAILS

#### Scenario: The gate's own validation was skipped on an event that may not skip it
- **GIVEN** the compute job concluded `skipped` on any event other than a label mutation
- **WHEN** the aggregating job runs
- **THEN** `required` FAILS, naming the event that skipped it

#### Scenario: An unregistered check run failed
- **GIVEN** every registered context succeeded
- **AND** an unregistered advisory check run on the same commit concluded `failure`
- **WHEN** `required` aggregates
- **THEN** `required` concludes success and the unregistered failure is not treated as gating

### Requirement: Failing closed applies at the deadline, and SHALL NOT wedge pull requests on transient states

A false RED is an outage in the same way a false green is: a gate that reds legitimate pull requests is
disabled by the people it blocks. The aggregation SHALL therefore distinguish a well-formed negative answer
(which is terminal) from a transient, self-correcting state (which is re-polled), and SHALL reach the same
fail-closed verdict at the deadline in both cases.

A registered tier whose latest check run concluded `cancelled` or `stale` SHALL be treated as non-terminal
while a superseding run is plausible, because supersession is routine: `cancel-in-progress` concurrency
cancels a tier's in-flight run whenever the pull request is re-pushed, labelled, or marked ready for review.
Supersession SHALL be recognised positively — the replacement run mints a higher check-run id, so the
cancelled run stops being the latest for that context. A cancellation for which no superseding run appears
SHALL FAIL, at a bounded grace window or at the deadline, whichever comes first, and SHALL NOT be waivable.

A TRANSPORT failure while reading the check-run set (5xx, secondary rate limit, DNS) SHALL be retried under
the existing backoff and SHALL NOT by itself end the aggregation. It SHALL fail closed once it persists —
to the deadline, or past a bounded consecutive-failure ceiling. A transport failure SHALL NEVER be read as
"no check runs" and SHALL NEVER produce a pass.

#### Scenario: A cancelled tier superseded by a green re-run passes
- **GIVEN** a registered tier's latest check run concluded `cancelled`
- **AND** on a later poll a higher-id check run for the same context concluded `success`
- **WHEN** the aggregation evaluates across those polls
- **THEN** it concludes success, and its summary records the superseding check run as the deciding one

#### Scenario: A cancellation with no successor still fails
- **GIVEN** a registered tier's latest check run concluded `cancelled` and no superseding run appears
- **WHEN** the grace window lapses, or the deadline arrives
- **THEN** the aggregation FAILS, names the tier, and states that no superseding run appeared
- **AND** a `ci:waive:<tier-id>` label does not excuse it

#### Scenario: A mid-poll API blip is retried rather than fatal
- **GIVEN** one check-run fetch fails while poll budget and deadline remain
- **AND** the next fetch succeeds and every registered tier has concluded `success`
- **WHEN** the aggregation evaluates
- **THEN** it concludes success and reports the transient failure as a warning

#### Scenario: A persistent fetch failure fails closed
- **GIVEN** every check-run fetch fails
- **WHEN** the consecutive-failure ceiling or the deadline is reached
- **THEN** the aggregation FAILS with a harness error naming the command, and never reports success

### Requirement: Registered tiers always emit their context, so absence is unambiguous

Every workflow registered as a gating tier SHALL emit its declared check-run context on EVERY pull request,
including pull requests the tier does not apply to. Inapplicability SHALL be reported as an explicit,
successful conclusion naming the reason; it SHALL NOT be reported by the absence of a check run.

A registered workflow SHALL NOT carry a trigger-level `paths:`/`paths-ignore:` filter that can prevent it
from starting (the always-fire sentinel form used by `ci.yml` is permitted). Applicability SHALL instead be
decided by a cheap, unconditional classifier job inside the workflow, following the pattern already used by
`observability-gate.yml` and by `pr-gate.yml`'s own docs-only classifier. The tier's expensive jobs SHALL
remain gated on that classifier's output so an inapplicable tier costs only the classifier.

#### Scenario: A registered tier that does not apply to the diff still reports
- **GIVEN** a PR whose diff touches nothing the tier covers
- **WHEN** the tier's workflow runs
- **THEN** its declared context is emitted with a successful conclusion naming the inapplicability reason
- **AND** the tier's expensive jobs did not run

#### Scenario: A registered workflow with a blocking trigger filter is rejected
- **GIVEN** a workflow listed in the registry whose `pull_request` trigger carries a real
  `paths:`/`paths-ignore:` filter
- **WHEN** the workflow-policy validation runs inside `required`
- **THEN** it FAILS and names the workflow and the filter

#### Scenario: A registered workflow with a blocking branch filter is rejected
- **GIVEN** a workflow listed in the registry whose `pull_request` trigger carries a `branches:` (or
  `branches-ignore:`) filter, so a pull request with another base would never start it
- **WHEN** the workflow-policy validation runs inside `required`
- **THEN** it FAILS and names the workflow and the filter

#### Scenario: A registered workflow whose activity types cannot cover every head sha is rejected
- **GIVEN** a workflow listed in the registry whose `pull_request.types` omits `opened` or `synchronize`,
  or which carries no `pull_request`/`pull_request_target` trigger at all
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and names the workflow and the missing types, because the context would be permanently
  absent for some head sha and every such pull request would deadlock for the whole deadline

#### Scenario: A registered workflow firing on an event the aggregator does not observe is rejected
- **GIVEN** a workflow listed in the registry whose `pull_request.types` includes an activity type absent
  from the aggregating workflow's own types
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and names the unobserved types, because such an event can cancel the tier's in-flight
  run with no `required` run watching for the replacement

#### Scenario: A registered workflow whose types are a subset of the aggregator's is accepted
- **GIVEN** a workflow listed in the registry whose `pull_request.types` covers `opened` and `synchronize`
  and is otherwise within the aggregating workflow's types
- **WHEN** the workflow-policy validation runs
- **THEN** it PASSES, because a rule that rejects a legitimate configuration wedges pull requests

#### Scenario: A registered workflow with no unconditional emitting job is rejected
- **GIVEN** a registered workflow in which every job emitting the declared context is conditional on
  something that can skip it
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and names the workflow and the declared context

#### Scenario: A compound `always() && …` condition is rejected
- **GIVEN** a registered workflow whose emitting job's condition merely CONTAINS `always()` — for example
  `always() && github.event.pull_request.draft == false`, which skips the job on every draft pull request
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, because the rule SHALL require a condition it can prove unconditional rather than one
  that mentions `always()`

#### Scenario: An emitting job that cannot report the tier's result is rejected
- **GIVEN** a registered workflow whose emitting job is unconditional but does not inspect the
  `result` of every job it depends on, or does not transitively depend on every other job in the
  workflow
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and names the job and the unreported dependency, because the context would report
  success regardless of that job's outcome

#### Scenario: A gate job whose only failing path is a comment or a quoted string is rejected
- **GIVEN** a registered workflow whose emitting job binds every `needs.<job>.result` into `env:` and only
  echoes them, with the token `exit 1` appearing solely in a shell comment or inside a quoted string
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, because the rule that exists to prevent an always-green tier SHALL NOT be satisfiable
  without preventing one: for every dependency, some step must both READ that dependency's result and be
  able to exit non-zero. Where a fully general proof is impractical the rule SHALL be conservative and
  reject what it cannot prove

### Requirement: A declared in-repo registry is the single source of truth and enrolment is forced

A registry file (`.github/ci-gating-tiers.yml`) SHALL declare every gating tier, each entry naming at
minimum a tier id, the owning workflow file, the exact check-run context, and an optional per-tier wait
override. `required` SHALL derive its expectation set from this registry alone and SHALL NOT infer
expectations from GitHub trigger semantics.

Enrolment SHALL be mechanically forced: the workflow-policy validation that runs as a step in the
`pr-gate-core` job — which the branch-protection context `required` declares in `needs:` and treats as an
unconditional failure unless it concluded `success` — SHALL fail when a workflow with a `pull_request` (or
`pull_request_target`) trigger is neither registered as a gating tier nor listed in the registry's exemption
block with a reason and an issue reference. The registry SHALL NOT list `pr-gate.yml`, so `required` can
never be registered against itself. A registry entry whose declared context no workflow emits SHALL be
rejected as dangling.

The aggregator SHALL additionally refuse, on its own account and independently of the enrolment rule, to
report success for an empty or unparseable `tiers:` list: a green aggregate over an empty expectation set is
the single "green with nothing checked" path the mechanism could otherwise take.

#### Scenario: The aggregator refuses a registry that would aggregate nothing
- **GIVEN** a registry whose `tiers:` key is absent, empty, or not a list
- **WHEN** the aggregation runs against it, with every other input healthy
- **THEN** it exits with a harness error rather than reporting that every registered tier succeeded

#### Scenario: A newly added PR-triggered workflow that is neither registered nor exempt reds `required`
- **GIVEN** a PR adding a new workflow with a `pull_request` trigger
- **AND** the registry lists it neither as a gating tier nor as an exemption
- **WHEN** `required` runs
- **THEN** `required` FAILS and names the unenrolled workflow

#### Scenario: An exemption without a reason is rejected
- **GIVEN** a registry exemption entry lacking a reason or an issue reference
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and names the entry

#### Scenario: A dangling registry entry is rejected
- **GIVEN** a registry entry whose declared context is emitted by no workflow in `.github/workflows/`
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and names the entry

#### Scenario: The registry cannot register the required gate itself
- **GIVEN** a registry entry whose workflow is `pr-gate.yml`
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS

#### Scenario: An empty tier list is rejected
- **GIVEN** a registry whose `tiers` list is empty, so `required` would aggregate nothing
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, because a vacuously green aggregate is the state this registry exists to prevent

#### Scenario: A context emitted by more than one workflow is rejected
- **GIVEN** a registry entry whose declared context is also the `name:` of a job in another workflow
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and names the other workflow, because a check-run name is global to the commit and
  a same-named sibling job could satisfy or shadow the registered tier

### Requirement: A diff that mandates a tier runs it regardless of labels

For a registered tier, the tier's classifier SHALL decide from the pull request's changed-file set whether
the tier is mandatory for that diff. When the diff mandates the tier, the tier's full jobs SHALL run and
its context SHALL reflect their result, **whether or not** the tier's `ci:*` opt-in label is present. The
label SHALL remain a manual opt-in only for diffs that do not mandate the tier.

A registered tier SHALL have **exactly one applicability verdict** governing every job behind its context.
A tier whose jobs are gated on more than one classifier output SHALL be rejected by the workflow-policy
rule: two predicates behind one context allow a diff to satisfy the narrower one, skip the work the tier
exists to run, and still see the context report success. Genuinely distinct scopes SHALL be registered as
separate tiers, each emitting its own context.

The tier's mandate SHALL cover every path that reaches the tier's subject at runtime or determines what its
tests assert — for the Flight tier: the Flight crate, the core engine it wraps, the test fixtures and
oracles its parity tests read, the workspace dependency manifests, the pinned toolchain, the shared CI
setup action, and the workflow itself. The registry's documented `mandate_paths` SHALL be checked against
the tier's classifier, and a documented path the classifier never mentions SHALL be rejected.

A worker SHALL NOT need per-pull-request knowledge of which tiers are out of band: no step of the delivery
flow SHALL require a human or agent to apply a label in order for a mandated tier to gate the merge.

#### Scenario: The #2906 case — a Flight change runs its e2e tier without the label
- **GIVEN** a PR whose diff touches `cqlite-flight/**` and which carries no `ci:flight-full` label
- **WHEN** its workflows run
- **THEN** the registered Flight tier's mandating jobs execute (including the end-to-end tests)
- **AND** `required` cannot conclude success until that tier reports success

#### Scenario: A core-only diff mandates the tier that owns the end-to-end tests
- **GIVEN** a PR whose diff touches only `cqlite-core/**` (or only the Cargo manifests, the pinned
  toolchain, `test-data/**`, or the shared CI setup action) and carries no `ci:flight-full` label
- **WHEN** the Flight tier's classifier evaluates the changed-file set
- **THEN** the verdict mandates the job that runs `cargo test --package cqlite-flight` — the integration
  and end-to-end tests — not merely the `--lib` job
- **AND** a diff touching only docs, the CLI, or the bindings does not mandate the tier

#### Scenario: A tier gated on two classifier outputs is rejected
- **GIVEN** a registered tier whose jobs are gated on two different `needs.<classifier>.outputs.*` values
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, naming the competing outputs and the jobs they gate

#### Scenario: A documented mandate path the classifier never mentions is rejected
- **GIVEN** a registry entry whose `mandate_paths` lists a path absent from the tier's workflow
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, naming the drifted path

#### Scenario: A non-mandating diff leaves the tier opt-in
- **GIVEN** a PR whose diff mandates no registered heavy tier and carries no `ci:*` label
- **WHEN** its workflows run
- **THEN** each registered tier emits an inapplicable-success context without running its expensive jobs
- **AND** `required` can conclude success

#### Scenario: The opt-in label still forces a non-mandated tier to run
- **GIVEN** a PR whose diff does not mandate a tier but which carries that tier's `ci:*` label
- **WHEN** its workflows run
- **THEN** the tier's full jobs run and its context reflects their result

### Requirement: The wait is bounded and expiry is a failure

The aggregation SHALL wait for non-terminal registered tiers under an explicit deadline (default 60
minutes, overridable per tier in the registry; the effective deadline is the maximum over registered
tiers). On expiry the aggregation SHALL FAIL, naming every tier that had not reached a terminal state.
It SHALL NEVER conclude success because it stopped waiting.

The aggregation deadline SHALL be strictly less than the enclosing job's `timeout-minutes`, so expiry
surfaces as a reported failure with a diagnostic rather than a job cancellation. Polling SHALL use a
backoff rather than a tight loop.

#### Scenario: A tier still pending at the deadline fails the gate
- **GIVEN** one registered tier is `in_progress` and the aggregation deadline has passed
- **WHEN** the aggregation evaluates
- **THEN** it FAILS with a non-zero exit and names the pending tier
- **AND** it does not report success anywhere in its output

#### Scenario: The deadline cannot exceed the job timeout
- **GIVEN** the configured aggregation deadline and the aggregating job's `timeout-minutes`
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS if the deadline is not strictly less than the job timeout

#### Scenario: A tier that becomes terminal before the deadline is observed
- **GIVEN** a registered tier that is non-terminal on the first poll and `success` on a later poll
- **WHEN** the aggregation evaluates with a poll budget allowing both observations
- **THEN** it observes the successful terminal state and concludes success

### Requirement: `required` never waits on its own check run

The aggregation SHALL exclude the check runs produced by its own workflow run by run identity, not by
matching a job or context name, so that renaming the job cannot either reintroduce a self-wait deadlock or
silently drop a real tier. Exclusion SHALL be derived from the executing run's own job set (the Actions
run-jobs API for `GITHUB_RUN_ID`) and SHALL additionally drop any check run whose details URL belongs to
that run id.

#### Scenario: The gate's own contexts are excluded
- **GIVEN** the PR head's check runs include those produced by the executing `pr-gate.yml` run
- **WHEN** the aggregation builds its observation set
- **THEN** every check run belonging to that run id is excluded
- **AND** the aggregation does not wait on any of them

#### Scenario: Renaming the required job does not reintroduce a self-wait
- **GIVEN** the aggregating job's `name:` and the sibling job's `name:` are changed
- **WHEN** the aggregation runs against the same check-run set
- **THEN** self-exclusion still removes exactly the executing run's check runs

### Requirement: Re-runs are observed without latching a stale result

For each registered context the aggregation SHALL evaluate the most recent check run only, requesting
GitHub's latest-per-name view and additionally preferring the highest check-run id when more than one
remains. It SHALL NOT latch a superseded failure, and SHALL NOT latch a superseded success.

The aggregation SHALL record, for every registered context, the `(context, check-run id, status,
conclusion, run URL)` it acted on, so the evidence behind a green `required` is inspectable afterwards.

#### Scenario: A re-run that turned green supersedes the earlier failure
- **GIVEN** two check runs for one registered context: an older one `failure` and a newer one `success`
- **WHEN** the aggregation evaluates
- **THEN** it observes `success` for that context

#### Scenario: A re-run in progress supersedes the earlier success
- **GIVEN** two check runs for one registered context: an older one `success` and a newer one `in_progress`
- **WHEN** the aggregation evaluates with the deadline already passed
- **THEN** it FAILS, treating the context as non-terminal

#### Scenario: The observed evidence is recorded
- **GIVEN** any aggregation outcome
- **WHEN** the job completes
- **THEN** its summary lists each registered context with the check-run id, status, conclusion, and run URL
  that decided it

### Requirement: A waiver can cover only an absent or pending tier, never a failed one

A per-tier break-glass label of the form `ci:waive:<tier-id>` MAY excuse a registered tier that is absent or
non-terminal at the deadline. It SHALL NEVER excuse a tier whose latest check run concluded `failure`,
`cancelled`, or `timed_out`. There SHALL be no blanket waiver that excuses all tiers at once.

The waiver SHALL BE REACHABLE, because the mechanism it excuses can wedge a pull request and that is
precisely when it is needed. Applying a label starts no run for an activity type the workflow does not
subscribe to, and re-running a workflow replays the ORIGINAL event payload; therefore the aggregating
workflow SHALL subscribe to `labeled`/`unlabeled` (enforced by the enrolment rule), and the aggregation
SHALL re-read the pull request's CURRENT labels on every poll rather than trusting the payload snapshot. A
failed label read SHALL fall back to the payload — withholding a waiver is safe, granting one is not.

An ABSENT waived tier SHALL be excused immediately rather than at the deadline: there is nothing to wait
for, so holding a runner for the full deadline only delays a verdict already determined. A PENDING waived
tier SHALL still be waited out, because it can still turn red, and a red tier is never waivable.

Each honoured waiver SHALL emit a warning annotation and a job-summary line naming the waived tier and the
actor, so a waived merge is visible after the fact.

#### Scenario: A waiver applied after the run started is honoured
- **GIVEN** a registered tier is absent and the pull request carried no waiver label when the run began
- **AND** `ci:waive:<tier-id>` is applied while the aggregation is polling
- **WHEN** the aggregation next evaluates
- **THEN** it re-reads the label from the API, excuses the tier, and does not require a re-run

#### Scenario: The aggregating workflow must observe label events
- **GIVEN** the aggregating workflow's `pull_request` trigger does not include `labeled`/`unlabeled`
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, because the documented break-glass would then be unexercisable

#### Scenario: A waived absent tier does not hold a runner for the whole deadline
- **GIVEN** a registered tier is absent and the pull request carries that tier's waiver label
- **WHEN** the aggregation evaluates with poll budget and deadline remaining
- **THEN** it excuses the tier immediately without waiting out the deadline
- **AND** a waived tier that is merely PENDING is still waited out

#### Scenario: A waiver excuses an absent tier
- **GIVEN** a registered tier is absent at the deadline and the PR carries that tier's `ci:waive:<tier-id>` label
- **WHEN** the aggregation evaluates
- **THEN** the tier is excused, a warning annotation names it, and `required` may conclude success

#### Scenario: A waiver cannot excuse a failed tier
- **GIVEN** a registered tier concluded `failure` and the PR carries that tier's `ci:waive:<tier-id>` label
- **WHEN** the aggregation evaluates
- **THEN** it FAILS and states that a failed tier cannot be waived

#### Scenario: A waiver is scoped to one tier
- **GIVEN** a PR carrying `ci:waive:<tier-a>` while `<tier-b>` is also absent
- **WHEN** the aggregation evaluates
- **THEN** it FAILS, naming `<tier-b>`

### Requirement: A label mutation SHALL NOT cancel, restart, or duplicate the gate

The aggregating workflow SHALL observe label events (so the waiver is reachable) **without** making routine
labelling expensive. A `labeled`/`unlabeled` event SHALL NOT cancel an in-flight run of the gate, and SHALL
NOT re-execute the gate's compute job. Two runs of the aggregating workflow for the same pull request SHALL
NOT both report a `required` conclusion; serialization SHALL be structural (a shared concurrency group),
not timing-dependent.

Reusing an already-recorded compute result SHALL NOT weaken it: the reuse SHALL read the result recorded
for the **same head sha**, excluding the reusing run's own check runs by run identity, and SHALL FAIL when
that result is absent, non-terminal, or anything other than success. The waiver SHALL take effect without a
manual re-run.

The workflow-policy validation SHALL reject an aggregating workflow that observes label events while
declaring an unconditional `cancel-in-progress: true`.

#### Scenario: A waiver applied mid-run takes effect without restarting the compute job
- **GIVEN** the gate's compute job is skipped for a `labeled` event and its result for this head sha is
  recorded as success
- **AND** a `ci:waive:<tier-id>` label is present for an absent tier
- **WHEN** the aggregation runs
- **THEN** it reuses the recorded compute result, reports the waiver, and concludes success
- **AND** no re-execution of the compute job is required

#### Scenario: A label event cannot manufacture a green compute result
- **GIVEN** the compute job is skipped for a label event
- **WHEN** the recorded result for this head sha is a failure, is absent, or is only the label run's own
  skipped check run
- **THEN** `required` FAILS

#### Scenario: Cancellation on label events is rejected by policy
- **GIVEN** the aggregating workflow subscribes to `labeled`/`unlabeled` and sets
  `concurrency.cancel-in-progress: true`
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, stating that every label mutation would cancel and restart the gate

### Requirement: The mechanism that decides `required` SHALL be read from the base ref

The aggregator, its supporting modules, and the gating-tier registry SHALL be evaluated from the pull
request's **base ref**, never from the pull request's own checkout, so that the pull request being gated
cannot rewrite the check that gates it. A registry change SHALL take effect only after it merges. The
pull request's diff SHALL still be classified normally (each tier's applicability is decided from the
head diff), and the enrolment policy SHALL still validate the head tree, since it is judging the proposed
change.

The aggregating job SHALL NOT adopt `pull_request_target`, and SHALL NOT execute pull-request-controlled
content in the privileged aggregation step. When the base ref carries no registry (bootstrap), the fallback
to the head copy SHALL be announced; it SHALL NOT be reachable when the base ref does carry one.

The workflow-policy validation SHALL reject an aggregating job that does not check out the base ref into
its own path, or that invokes the workspace-root copy of the aggregator.

#### Scenario: A PR that moves its own tier to `exempt:` is still gated
- **GIVEN** the pull request's registry moves a registered tier into `exempt:` and that tier's context is
  absent from the head
- **WHEN** the aggregation is evaluated against the base ref's registry
- **THEN** it FAILS and names the absent tier
- **AND** the same evidence evaluated against the pull request's own registry would have passed

#### Scenario: An aggregator reading its own PR's copy is rejected
- **GIVEN** the aggregating job runs the workspace-root copy of the aggregator, or performs no base-ref
  checkout
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, stating that the check would be defined by the thing it checks

### Requirement: A green `required` is what releases `--auto`, and it implies every expected tier already passed

The delivery doctrine SHALL state accurately what `required` covers. `CLAUDE.md`'s autonomy section and
`website/src/content/docs/agents-developing/gate-contract.md` SHALL record that arming
`gh pr merge --auto` before tiers finish remains correct because GitHub releases the merge on the
`required` context turning green, and that `required` cannot turn green until every registered gating tier
has reported success — and SHALL state the residual: re-running a tier after `required` is green requires
re-running `required`.

The documented rule SHALL be mechanical — expressible without per-pull-request judgement about which tiers
are out of band.

#### Scenario: Doctrine states the coverage that the mechanism actually provides
- **GIVEN** the merged change
- **WHEN** `CLAUDE.md`'s autonomy section and the `gate-contract` page are read
- **THEN** both state that `required` aggregates the registered sibling tiers and fails closed on any that
  is failed, pending, or absent
- **AND** both state the tier-then-`required` re-run order

#### Scenario: A worker needs no per-PR tier knowledge
- **GIVEN** the documented merge procedure
- **WHEN** a worker follows it for any PR
- **THEN** no step asks the worker to determine which tiers are out of band or to apply a tier label

### Requirement: The aggregation logic is testable offline and every guard is proven able to fail

The aggregation SHALL be implemented as a script whose check-run input, registry path, deadline, and poll
budget are injectable, so its full decision surface can be exercised offline against synthetic check-run
fixtures with no network access and no sleeping. The clock used to age a superseded conclusion and the
label source SHALL be injectable for the same reason. A test suite SHALL cover at least: all-pass,
one-pending, one-failed, one-absent-and-registered, one-absent-and-not-registered, duplicate check runs for
one context (re-run), self-exclusion, each waiver case, cancelled-then-superseded, cancelled-with-no-
successor, a transient fetch failure that recovers, one that does not, and a registry that would aggregate
nothing.

Each fix SHALL carry a discriminating mutant, and the mutant set SHALL include the near-miss INVERSES that
must NOT be rejected — a legitimate `types:` subset, the `${{ always() }}` spelling, a genuine failing path
on a line containing a quoted `#` — because a rule that reds a legitimate configuration is an outage too.

Every state SHALL have a **discriminating** case that proves the guard FAILs when it should — asserting a
non-zero exit and the naming of the offending tier, not merely that a passing case passes. Non-vacuity
SHALL itself be proven: replacing the aggregation script with a stub that always exits 0 SHALL turn the
suite RED, and replacing the enrolment policy rule with an always-pass stub SHALL turn the workflow-policy
tests RED.

Assertions SHALL be made on observed check-run states and exit codes only, never on elapsed wall-clock time
(#2642); deadline expiry SHALL be exercised by injecting an already-expired deadline or a zero poll budget.
The suite SHALL run in the local gate's `tooling-tests` component.

#### Scenario: Synthetic fixtures drive every state offline
- **GIVEN** synthetic check-run fixtures for all-pass, one-pending, one-failed, one-absent-and-registered,
  and one-absent-and-not-registered
- **WHEN** the test suite runs with no network access
- **THEN** each state produces the specified verdict, and the four non-passing states each exit non-zero

#### Scenario: An always-pass stub aggregator makes the suite fail
- **GIVEN** the aggregation script is replaced with a stub that exits 0 unconditionally
- **WHEN** the test suite runs
- **THEN** the suite FAILS

#### Scenario: An always-pass stub enrolment rule makes the policy tests fail
- **GIVEN** the enrolment rule in the workflow-policy validation is replaced with an always-pass stub
- **WHEN** the workflow-policy tests run
- **THEN** they FAIL

#### Scenario: No test asserts on wall-clock time
- **GIVEN** the new test suite
- **WHEN** the repository's wall-clock-assert guard runs over it
- **THEN** it reports no wall-clock threshold assertion in the correctness test path

#### Scenario: The suite is wired into the gate
- **GIVEN** a full `scripts/agent-gate.sh` run
- **WHEN** the `tooling-tests` component executes
- **THEN** it invokes the new test suite and a failure there fails the component
