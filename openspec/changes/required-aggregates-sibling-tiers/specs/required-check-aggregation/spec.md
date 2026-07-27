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

#### Scenario: A compound condition is rejected
- **GIVEN** a registered workflow whose emitting job's condition merely CONTAINS the mandated function —
  for example `!cancelled() && github.event.pull_request.draft == false`, which skips the job on every
  draft pull request
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, because the rule SHALL require a condition it can prove unconditional rather than one
  that merely mentions the function

#### Scenario: An emitting job SHALL NOT launder a cancellation into a failure
- **GIVEN** a registered workflow whose emitting job is conditioned on `always()`
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS and states that `always()` runs the gate job while the run is BEING CANCELLED, at which
  point every `needs.<job>.result` is `cancelled` and the gate's own check turns that into a `failure`
  conclusion — so the supersession grace could never fire and a routine supersession would red `required`
- **AND** the mandated condition SHALL be one that does not run during a cancellation while still running
  when a dependency failed or was skipped, so the context is still emitted on every pull request
- **AND** the aggregation SHALL treat every conclusion GitHub may record for a job that did not run in a
  cancelled run — including `skipped` — as non-terminal for the bounded grace and as a FAILURE afterwards,
  since which spelling GitHub uses is not verifiable offline and no spelling may pass

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
tier SHALL still be waited out, because it can still turn red, and a red tier is never waivable — EXCEPT
where the pending state was created by the waiver's own label event: when the tier's only check run was
minted at or after the moment the waiver label was applied, it cannot be information the waiver's author
lacked, and the waiver SHALL resolve at once. Without a resolved label-event time there SHALL be no such
horizon, so an unreadable event feed can only ever withhold a waiver, never grant one.

Each honoured waiver SHALL emit a warning annotation and a job-summary line naming the waived tier and the
person who applied the label, so a waived merge is visible after the fact. The attribution SHALL be
resolved from the pull request's `labeled` events (the most recent application of that label wins); it
SHALL NOT name the actor of the event that started the aggregating run, who is generally not the labeller.
Where the attribution cannot be resolved, the diagnostic SHALL state that it is unresolved rather than
name anyone, and the resolved login SHALL be allowlisted before it reaches a workflow command.

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

#### Scenario: The waiver is attributed to the labeller, not to this run's actor
- **GIVEN** a waived tier, a `labeled` event recording who applied `ci:waive:<tier-id>`, and a different
  actor for the event that started the aggregating run
- **WHEN** the aggregation reports the honoured waiver
- **THEN** the annotation and summary name the labeller
- **AND** the run's actor appears nowhere in the output

#### Scenario: An unresolvable attribution claims no name
- **GIVEN** a waived tier whose label-event feed cannot be read
- **WHEN** the aggregation reports the honoured waiver
- **THEN** the tier is still waived
- **AND** the diagnostic states that the applier is unresolved instead of naming anyone

#### Scenario: A waiver resolves the pending tier its own label event started
- **GIVEN** a waived tier whose only check run was minted at or after the waiver label was applied
- **WHEN** the aggregation evaluates with deadline and poll budget remaining
- **THEN** it excuses the tier immediately, stating why
- **AND** the same tier pending from a run that PREDATES the waiver is still waited out

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

The same protection SHALL extend to every REGISTERED TIER that observes label events. On a tier the
consequence is worse than a wasted re-run: applying `ci:waive:<tier-id>` to a wedged pull request would
cancel that tier's in-flight run and mint a fresh `queued` check run, so the break-glass would fight the
very tier it waives.

The workflow-policy validation SHALL reject an aggregating workflow OR a registered tier that observes
label events while its `cancel-in-progress` is not provably false for them. Rejecting only the literal
`true` is insufficient: an expression such as `${{ github.event_name == 'pull_request' }}` is TRUE for
`labeled`/`unlabeled` and behaves identically. The accepted forms are `false`, absence, or an expression
that is ACTION-AWARE — one that references the event action and names both label activity types.

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

#### Scenario: A registered tier that cancels on a label event is rejected by policy
- **GIVEN** a registered tier subscribes to `labeled`/`unlabeled` and its `cancel-in-progress` is `true`,
  or an expression such as `${{ github.event_name == 'pull_request' }}` that is true for a label event
- **WHEN** the workflow-policy validation runs
- **THEN** it FAILS, stating that applying the waiver would cancel the tier being waived

#### Scenario: An action-aware cancellation on a registered tier is accepted
- **GIVEN** a registered tier whose `cancel-in-progress` excludes `labeled` and `unlabeled` by action
- **WHEN** the workflow-policy validation runs
- **THEN** it passes, so a new head sha still cancels an obsolete run

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

### Requirement: A tier the running tree cannot emit SHALL red immediately, naming the remedy

Reading the registry from the base ref separates WHERE THE REGISTRY LIVES from WHERE THE EMITTER LIVES.
When a tier registered on the base ref cannot be emitted by the tree the event actually ran — its workflow
is absent, it has no pull-request trigger, its `types:` exclude this event, its `branches:` exclude this
base, or no job carries the declared context as its name — the context can never arrive. The aggregation
SHALL detect that state and FAIL immediately with a diagnostic naming both remedies (rebase onto the base
branch, or the documented per-tier waiver for a deliberate rename or retirement). It SHALL NOT poll such a
context to the aggregation deadline.

Detection SHALL rest on provable properties only. Any inconclusive evidence — an unparseable workflow, a
computed job name, a filter whose outcome depends on the diff, or an unavailable copy of the tree — SHALL
yield no verdict and fall back to ordinary polling, because a false "cannot emit" is a false red.

The verdict SHALL only ever be a failure. "The running tree cannot emit this tier, therefore pass" SHALL
NOT exist, since the pull request controls that tree.

The aggregation SHALL NOT bound the absence of a registered context by a short timer: a tier's emitting job
depends on every other job in its workflow, so its check run legitimately does not exist for as long as the
tier takes to run, and any such timer would red exactly the pull requests that genuinely mandate the tier.

The workflow-policy validation SHALL reject an aggregating job that does not read the tree the event ran,
or that reads it and does not pass it to the aggregation.

#### Scenario: A pull request that renames a registered tier's context reds at once
- **GIVEN** the base ref registers a tier by context name, and the tree the event ran emits a different
  name for it
- **WHEN** the aggregation runs
- **THEN** it FAILS on the first poll, states that this is a migration state, and names both the rebase and
  the per-tier waiver as remedies
- **AND** no polling interval is consumed

#### Scenario: A deliberate rename ships via the waiver
- **GIVEN** the migration state above and the tier's waiver label applied
- **WHEN** the aggregation runs
- **THEN** it PASSES and records the waiver, because a registry change takes effect only once merged

#### Scenario: Inconclusive evidence does not red a pull request
- **GIVEN** the tree the event ran is unavailable, unparseable, or names the emitting job with an
  expression that cannot be resolved
- **WHEN** the aggregation runs
- **THEN** no migration verdict is produced, the situation is announced, and the tier is polled normally

#### Scenario: The detection cannot turn a failure green
- **GIVEN** a registered tier that reported a failing conclusion, in a tree that also cannot emit it
- **WHEN** the aggregation runs
- **THEN** it FAILS

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

### Requirement: A registered tier SHALL be satisfied only by a check run GitHub Actions produced

A check run is identified to branch protection by NAME ALONE, and any application holding `checks:write`
on the repository can create one. Global name-uniqueness across workflow FILES does not close that: it
cannot see a check run minted through the Checks API. The aggregation SHALL therefore verify the PRODUCER
of every check run it acts on — for a registered tier's context and for the gate's own recorded compute
result alike — and SHALL accept only GitHub Actions, identified by the check run's `app` (slug or id) with
its details URL pointing at an Actions workflow run.

Verification SHALL FAIL CLOSED: a check run whose producer cannot be established SHALL NOT satisfy the
tier, and SHALL NOT SHADOW a genuine check run of the same name either, whatever its check-run id. When no
genuine check run remains, the aggregation SHALL FAIL naming the unverifiable one and the reason.

#### Scenario: A same-named check run from another app does not satisfy the tier
- **GIVEN** a registered tier's context exists on the head as a `success` check run created by an app other
  than GitHub Actions
- **WHEN** the aggregation evaluates
- **THEN** it FAILS, names the tier, and states that the run was not produced by GitHub Actions

#### Scenario: A check run with no identifiable producer fails closed
- **GIVEN** a check run carrying a registered tier's context but no `app` information, or a details URL that
  does not resolve to an Actions workflow run
- **WHEN** the aggregation evaluates
- **THEN** it FAILS rather than accepting the run

#### Scenario: An unverifiable check run cannot shadow the genuine one
- **GIVEN** a genuine Actions check run for a registered tier concluded `failure`, and a higher-id check run
  of the same name from another app concluded `success`
- **WHEN** the aggregation evaluates
- **THEN** it FAILS; the higher id does not make the forgery the observed result

### Requirement: A tier's applicability verdict SHALL be validated, not assumed

A registered tier reports inapplicability as an emitted success, and its gate job treats a skipped
dependency as a pass ONLY on the strength of the classifier's applicability verdict. The gate job SHALL
therefore validate that verdict: a value that is neither of the two booleans — empty, unwritten, or any
other string — SHALL FAIL the tier rather than being read as "not applicable". When the verdict says the
tier DOES apply, the gate SHALL additionally require that the tier's work actually ran to success; a
mandating diff whose jobs were skipped SHALL NOT report a green tier.

#### Scenario: An unreadable applicability verdict reds the tier
- **GIVEN** a registered tier whose classifier concluded success but whose applicability output is empty or
  not a boolean
- **WHEN** the gate job evaluates
- **THEN** the tier's context concludes `failure`, naming the unreadable verdict
- **AND** it does not report "not applicable to this diff"

#### Scenario: A verdict that claims applicability with skipped work reds the tier
- **GIVEN** a registered tier whose applicability verdict is `true` while its work jobs are `skipped`
- **WHEN** the gate job evaluates
- **THEN** the tier's context concludes `failure`

### Requirement: The gating mechanism SHALL declare and check its interpreter floor in one place

The gating mechanism has a single implementation language, so its interpreter version is load-bearing: a
host below the floor would not fail loudly, it would MIS-RUN (a missing `Enumerable#filter_map`, or a YAML
loader silently rejecting the alias keyword, each swallowed by a rescue that then reports "inconclusive").
The floor SHALL be declared in exactly one file, and every gating entry point SHALL go through that
declaration. A library caller below the floor SHALL abort with a message naming the floor, the constructs
that set it, and the remedy. The self-tests SHALL SKIP with that reason rather than run against an
interpreter that cannot execute them faithfully.

#### Scenario: A host below the declared floor fails loudly
- **GIVEN** an interpreter older than the declared floor
- **WHEN** the aggregation is invoked
- **THEN** it fails closed with a diagnostic naming the floor and the remedy, and never reports a verdict

#### Scenario: Every gating entry point goes through the single declaration
- **GIVEN** the set of gating source files
- **WHEN** they are inspected
- **THEN** each one requires the single floor declaration

### Requirement: The trust boundary's complementary ownership control SHALL exist and SHALL NOT be overstated

Base-ref evaluation guarantees the SET of tiers, their contexts and the aggregation logic; it does not
govern the code a tier executes. That residual SHALL be documented with the control that actually applies
to it. A `CODEOWNERS` file SHALL exist at a location GitHub honours and SHALL assign owners for the CI
mechanism trees (`.github/` and `scripts/ci/`), so a trust-boundary diff raises an automatic review
request. The documentation SHALL state the control's real strength — including whether code-owner review
is enforced by live branch protection — rather than implying enforcement that is not configured.

#### Scenario: Code owners cover the trust-boundary trees
- **GIVEN** the repository's CODEOWNERS file
- **WHEN** the gating self-tests inspect it
- **THEN** both `.github/` and `scripts/ci/` are covered by a rule naming at least one owner
- **AND** removing either rule reds the check
