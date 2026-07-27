# Design — `required` aggregates sibling tiers

Issue #2910. Related: #2433 (single required context + `enforce_admins`), #2667 (arm `--auto`, GitHub owns
the green-wait), #1310 (gate PASS != CI green), #1269 (gate components vs CI lanes), #2645/#2644 (the
always-emit docs-only classifier already inside `required`).

## The crux: "no check run present" is ambiguous

An absent context means BOTH "this tier legitimately does not apply to this diff" and "the workflow failed
to trigger". Resolve it wrong toward strictness and every PR blocks forever; wrong toward leniency and the
hole is exactly as open as today. Three ways to disambiguate were considered.

### (a) Parse each workflow's `paths:`/label conditions and compare against the diff

`required` would read all 25 workflows and decide, for each, whether GitHub *should* have started it.
Self-maintaining — a new tier needs no enrolment step.

Rejected. It builds a **second oracle for GitHub's own trigger semantics**: `paths` vs `paths-ignore`
precedence, `**`/`!` glob semantics, `branches`, `types` (including the `labeled`/`unlabeled` re-trigger
several tiers rely on), draft/`ready_for_review` behaviour, fork restrictions, `merge_group`, and
per-job `if:` expressions that are arbitrary GitHub expression-language. Every one of those is a place the
parser can diverge from GitHub — and the divergence's failure mode is **silent-open**: the parser concludes
"not applicable", `required` goes green, and the tier never ran. That is precisely the #2910 defect
reproduced inside the fix. Worse, the diverging cases are the interesting ones: a `paths:` filter that
silently stops matching after a refactor is exactly the incident this change exists to catch, and (a)
would agree with the broken filter.

### (b) Every gating tier always emits a check run

Absence becomes unambiguous — always an error — and `required` needs zero filter knowledge. The failure
mode inverts to **loud-closed**.

Insufficient alone: a new tier that forgets to always-emit is invisible again, with no signal. The #2910
brief names this exact gap ("unless there is a mechanism forcing enrolment").

### (c) CHOSEN — registry + always-emit, with enrolment forced inside `required`

`.github/ci-gating-tiers.yml` declares the gating tiers; each registered tier always emits its context;
`required` fails closed on any registered context that is absent, non-terminal, or failed; and a
workflow-policy rule forces every `pull_request` workflow to be registered or explicitly exempted.

**Why (c) over (a):** the deciding argument is failure-mode asymmetry, not maintenance cost.

| | (a) parse filters | (c) registry + always-emit |
| --- | --- | --- |
| Tier silently stops matching its filter | parser agrees → **silent green** | context absent → **red, named** |
| New tier not enrolled | covered automatically | policy rule reds `required` |
| Mechanism bug | reports "not applicable" → open | reports absent → closed |
| Second oracle for GH semantics | yes | no |

(c) has no state in which the mechanism's own bug produces a false green. That is worth the enrolment cost.

**Why (c) over (b) alone:** the registry is the forcing function. It is a single in-repo list, and
`validate-workflows.rb` — which already runs *as a step in the `pr-gate-core` job that the
branch-protection context `required` needs and refuses to pass without* and already carries
structural per-workflow guards (`WORKFLOW_GUARDS`, `observability_classifier_valid?`) and
reason-annotated exemption tables — is the natural, unbypassable home for the rule.

**Why the repo is already shaped for this.** Both halves of always-emit exist here today:
`ci.yml` uses `paths-ignore: ['__required_ci_context_never_matches__']` to make an otherwise-filtered
workflow always fire, and `observability-gate.yml` runs an unfiltered cheap `classify` job whose outputs
gate the heavy jobs. `pr-gate.yml`'s own docs-only classifier is a third instance. `docs/ci/ci-tier-policy.md`
already forbids path-filtered required checks. This change generalises an established local pattern rather
than importing a new one.

**Fail-closed on the ambiguous case:** an absent registered context is a FAILURE, unconditionally. There is
no "probably not applicable" branch anywhere in the aggregator.

## The label half of the hole

Always-emit alone would have let #2906 through: `flight-ci.yml`'s classifier would have emitted "not
applicable — `ci:flight-full` absent" as a success. So for a **registered** tier the applicability decision
is a diff-based *mandate* owned by the tier's own classifier, and it **overrides** the label:

- diff mandates the tier → the heavy jobs run, label or no label;
- diff does not mandate it → the `ci:*` label remains a manual opt-in;
- either way the context is emitted, so `required` needs no knowledge of which rule fired.

The mandate predicate lives with the tier (colocated with the people who own it) and is evaluated against a
plain `git diff --name-only base...head` — the same set `required`'s existing classifier computes. This is
not a reimplementation of GitHub trigger semantics: it is a first-class, single-owner predicate whose
failure mode (too broad) costs CI minutes, not correctness.

**One predicate, not two (review round 2).** The first cut of the Flight tier published *two* classifier
outputs — `run_tier` (fmt/clippy/`--lib`) and `run_full` (the ~30 e2e tests in `cqlite-flight/tests/`) —
behind one context, governed by two overlapping path regexes. A `cqlite-core/**`-only diff matched only the
narrower one, so the e2e tests never ran and `Flight tier gate` reported success: **#2906 reappearing
inside its own fix**, in the exact direction Flight actually breaks from (#2825 shipped a wrong
`worst_case_batch_capacity_bytes` because nothing exercised it; #2821 was its first caller). Two predicates
behind one context means the weaker one can silently win, so:

- a registered tier has **exactly one applicability verdict**, enforced by `gating_policy_rules.rb`
  (`applicability_scope_errors`); genuinely distinct scopes must be two registered tiers with two contexts;
- the mandate set is everything that reaches the tier at runtime or decides what its tests assert —
  for Flight: `cqlite-flight/**`, `cqlite-core/**`, `test-data/**`, `Cargo.toml`, `Cargo.lock`,
  `rust-toolchain.toml`, `.github/actions/setup-rust-ci/**`, and the workflow itself;
- the registry's `mandate_paths` is prose, so `mandate_path_errors` fails the gate when a documented path
  is not even mentioned by the tier's classifier (documented-but-unimplemented coverage is worse than none).

## Label events must not tax the gate (review round 2)

Subscribing the aggregator to `labeled`/`unlabeled` is what makes the `ci:waive:<tier-id>` break-glass
reachable — but combined with the pre-existing `cancel-in-progress: true` it meant **every** label mutation
(`ci:perf`, a board-mirror label, `needs-decision`, or the waiver itself) cancelled the in-flight run and
restarted the 30-minute core. Applying the escape hatch would have cost a full gate re-run.

Three options were weighed: (i) conditional `cancel-in-progress`; (ii) a separate concurrency group for
label runs; (iii) split so a label event re-evaluates only the aggregator. **(i) + (iii) chosen; (ii)
rejected** — a separate group permits two concurrent runs of the same workflow on the same head, and both
would report a `required` conclusion, with last-writer-wins deciding a merge. Concretely:

- the concurrency **group is unchanged**, and that is the serialization guarantee: at most one run per PR
  executes, so two runs can never both report `required` (a queued run reports nothing until it starts);
- `cancel-in-progress` becomes `github.event.action != 'labeled' && != 'unlabeled'` — a new head sha still
  cancels the obsolete run; a label mutation queues behind it;
- `pr-gate-core` is **skipped** on label events, so the heavy work never re-runs. Skipping the WORK never
  skips the CHECK: `required` requires the `pr-gate-core` result **already recorded for the same head sha**
  (`check_name=pr-gate-core&filter=all`, minus this run's own job ids — `filter=latest` would return only
  the label run's own `skipped` check run and hide the real one). Absent, pending, failed, or skipped-on-a-
  non-label-event all fail closed, so a label can never manufacture a green core;
- the waiver needs no new run at all in the common case: the aggregator re-reads the PR's live labels every
  poll, so a waiver applied mid-wait takes effect in the already-running job. The label-triggered run is the
  path for a waiver applied *after* `required` concluded.

`aggregator_concurrency_errors` rejects an unconditional `cancel-in-progress: true` while the aggregator
observes label events, so this cannot regress silently.

## The trust boundary: base ref decides (review round 2)

`required` is the sole branch-protection context, and it was reading `aggregate-required-tiers.sh`,
`gating_registry.rb` and `.github/ci-gating-tiers.yml` **from the PR's own checkout** — the check was
defined by the thing it checks. A PR could gut the aggregator, or move its own tier from `tiers:` to
`exempt:` (the enrolment rule accepts any reason ≥ 10 chars plus an issue ref), and go green on
instructions it wrote. Human review is a weak backstop for the one mechanism whose purpose is not relying
on a human noticing.

The `required` job now checks out `github.event.pull_request.base.sha` into `base-gating/` and runs the
aggregator from there; the script derives its registry and ruby modules from its own location, so the whole
decision surface is the base ref's. A registry change takes effect **after it merges**, which is the
correct semantics. Details:

- **Not `pull_request_target`.** It runs the base workflow file (closing more) but hands a base-context
  token to a run that must also see head content — the classic pwn-request shape, and a much larger blast
  radius than the hole being closed. Checking out the base copy of the specific trusted files is the
  smaller instrument. The `required` job checks out **no head content at all**.
- **Bootstrap.** When the base ref carries no registry (the PR that introduces the mechanism; a PR onto a
  branch cut before it), the job falls back to the head copy with a `::warning::`. This cannot be used to
  escape a base registry that exists — the base's state is not PR-controllable, so a PR that *deletes* the
  registry still faces the base copy.
- **Policy vs aggregation.** `validate-workflows.rb` deliberately still validates the **head** tree: it is
  judging the proposed change (a PR adding a workflow *and* enrolling it must pass). Aggregation uses the
  base. Consequence to know: renaming a base-registered tier's **context** in the same PR makes that
  context absent under the base registry, so such a rename is a two-PR operation or uses `ci:waive:<id>`.
- **Residual, stated rather than hidden.** Two things stay head-controlled, and neither is closable by a
  base-ref *checkout*:
  1. the code a tier executes — a tier's classifier can be edited to report "not applicable";
  2. **the aggregating workflow file itself**. `pull_request` runs the head's `pr-gate.yml`, so a PR can
     in principle delete the base checkout, or the `validate-workflows.rb` step that enforces it. Only
     `pull_request_target` (rejected above) or repository settings close that.

  What base-ref evaluation *does* guarantee is that the SET of tiers, their contexts, and the aggregation
  logic cannot be swapped out by an ordinary registry/script edit — the cheap, reviewable-looking change.
  The remaining path is a conspicuous diff to `.github/workflows/pr-gate.yml`, and
  `aggregator_trust_boundary_errors` keeps the base-ref checkout from being dropped by accident rather
  than by intent.

- **Provenance, and the precedent already in branch protection (round 4).** A registered tier used to be
  satisfied by ANY check run with the declared name. Branch protection itself does not work that way: the
  live `required_status_checks.checks` entry is `{context: required, app_id: 15368}` — GitHub pins the
  producing app for the one context it enforces. Nothing extended that pinning to the tiers `required`
  aggregates, so anything holding `checks:write` could mint `Flight tier gate` with `conclusion: success`
  and satisfy a tier. The aggregation now verifies the producer itself (`app.slug`/`app.id` = GitHub
  Actions, plus an Actions run `details_url`), fail-closed, for tier contexts and for the recorded
  `pr-gate-core` result alike; an unverifiable run neither satisfies a tier nor SHADOWS the genuine one,
  whatever its check-run id. Cost stated: this makes the `app` field of the check-runs API load-bearing —
  if GitHub stopped returning it every tier would red. That is the correct direction for a merge gate,
  and `ci:waive:<tier-id>` is the hatch.

- **The "CODEOWNERS" control, corrected (round 4).** Earlier drafts of this section closed by naming
  "CODEOWNERS on `.github/` + `scripts/ci/`" as the complementary control for that residual. **There was
  no CODEOWNERS file anywhere in the repo** — not `.github/CODEOWNERS`, not `/CODEOWNERS`, not
  `docs/CODEOWNERS` — so `require_code_owner_reviews` had nothing to resolve and the named control did
  not exist. A design must not delegate its one acknowledged residual to a control that does not exist,
  so this round makes the ownership real *and* states its true strength:
  - `.github/CODEOWNERS` now assigns `/.github/` and `/scripts/ci/` to `@pmcfadin`. GitHub honours a
    CODEOWNERS file at `.github/CODEOWNERS` (also `/CODEOWNERS`, `docs/CODEOWNERS`); the syntax is
    `<pattern> <owner>…`, last match wins. Validated against GitHub's own
    `GET /repos/{owner}/{repo}/codeowners/errors` endpoint, which reports zero errors for it.
  - Effect **today**: an automatic review REQUEST on every PR touching those trees — visibility, not
    enforcement.
  - Not enforced: the LIVE branch protection on `main` has `require_code_owner_reviews: false` and
    `required_approving_review_count: 0`. `require_code_owner_reviews` was therefore inert
    *independently of this change*, and the checked-in `.github/branch-protection.json` (which says
    `true`, with `require_last_push_approval: true` and one required approval) has **drifted from the
    live settings**. That drift predates this change and is recorded here rather than silently fixed:
    flipping it on would require a human approval on every agent PR touching `.github/**`, which
    contradicts the merge-on-green autonomy doctrine in `CLAUDE.md` and is an owner decision.
  - Honest statement of the residual: **visible, but uncontrolled at merge time.** The mechanised
    controls are base-ref evaluation, `aggregator_trust_boundary_errors`, and the enrolment rule; the
    review request is a notification on top of them.
  - `scripts/tests/test_gating_registry_policy.sh` asserts the file exists and covers both trees, so
    the control cannot silently disappear again (the mutant deletes the `/.github/` rule and the suite
    reds).

## Mechanics

**Where the aggregation runs.** `pr-gate.yml` splits into `pr-gate-core` (today's steps, unchanged) and
`required` (`name: required` preserved, `needs: [pr-gate-core]`, `if: always()`), whose only work is
aggregation. Rationale: the required *context name* is unchanged, so branch protection is untouched; the
aggregator gets its own generous `timeout-minutes` without inflating the compute job's; and `if: always()`
guarantees the context still reports when core fails. `required` fails whenever `pr-gate-core` did not
conclude `success` — the aggregate can never mask the core.

### Cost, stated honestly (review round 2)

An earlier draft of this section claimed the split "releases the heavy runner before the wait begins".
That was **wrong**, and an inaccurate cost note is how the next person mis-plans capacity. The split
separates *timeouts*, not runners. The true cost:

- `required` occupies a **second `ubuntu-latest` runner** from the moment `pr-gate-core` concludes until the
  slowest registered tier reaches a terminal state — worst case the 60-minute deadline, then a reported red
  (the job's own 75-minute timeout is the backstop).
- What the split *does* keep cheap is that job's shape: a bare checkout of the base ref, no toolchain, no
  cargo cache, no build. It is the cheapest runner-minute available, and on a public repo `ubuntu-latest`
  minutes are free — the real cost is queue contention when many PRs are open.
- Polling backs off 15s → 60s, so a 60-minute wait is ~60 API calls, not a tight loop.
- The common case is much cheaper than the worst: tiers start at the same time as `pr-gate-core` (~10–25
  min), so when they finish first the aggregation's **first poll decides** and the job exits in under a
  minute. Widening the Flight mandate to `cqlite-core/**` (round 2, Q1) makes the Flight `full` tier —
  release build plus ~30 e2e tests — the likely long pole on core-touching PRs, so expect `required` to
  genuinely wait on those.
- Rejected for now: making the wait free by moving aggregation to a `workflow_run`-triggered job that
  *writes* the `required` check run. It removes the idle runner but needs a check-run writer with
  `checks: write`, a `pull_request` association lookup, and a story for the run that never fires — i.e. it
  re-opens the never-fires trap this design exists to close. Tracked as a follow-up, not a prerequisite.

**What it polls.** `GET /repos/{owner}/{repo}/commits/{head_sha}/check-runs?filter=latest`, paginated, with
`head_sha = github.event.pull_request.head.sha` — the PR head, **not** `github.sha` (the synthesised merge
commit for `pull_request` events). Branch protection evaluates contexts against the PR head, so that is the
association key; task 6.1 asserts this empirically on the change's own PR before the registry is populated,
because getting it wrong yields a permanently-empty check-run set (which fails closed, loudly — the safe
direction, but useless).

**Permissions.** `pr-gate.yml` declares `permissions: contents: read` today, which *narrows* the token
below the repo default (`default_workflow_permissions: read`) and leaves it unable to read check runs. It
gains `checks: read` (the check-runs endpoint) and `actions: read` (the run-jobs endpoint used for
self-exclusion). Both are read-only and available to the default `GITHUB_TOKEN` on fork PRs; no PAT is
needed, so this does not join `PROJECTS_TOKEN`/`PARITY_HEAL_TOKEN` in the fail-loud-if-absent class.

**Timeout.** A single aggregation deadline (default 60 min, per-tier override in the registry, taken as the
max over registered tiers) bounds the wait. Expiry is a **FAILURE** that names every non-terminal tier —
never a pass, never a silent give-up. The deadline is strictly less than the `required` job's
`timeout-minutes` so expiry surfaces as a red check with a diagnostic rather than a job cancellation
(cancelled also blocks the merge, but reports nothing actionable). Polling backs off (15s → 60s) so a
long wait is cheap.

**Self-exclusion.** Name matching is fragile — a rename would silently re-open the hole or deadlock the
job on itself. Instead the aggregator excludes by **identity of its own workflow run**: it fetches
`GET /repos/{o}/{r}/actions/runs/${GITHUB_RUN_ID}/jobs` and drops every check run whose `id` is in that
job-id set (an Actions job and its check run share the same numeric id), with a secondary drop of any
check run whose `details_url` contains `/actions/runs/${GITHUB_RUN_ID}/`. Both are name-independent.
A third, structural guard: `validate-workflows.rb` rejects a registry entry naming `pr-gate.yml`, so
`required` can never be registered against itself even by hand.

**Re-runs.** `filter=latest` asks GitHub for the newest check run per name; the aggregator additionally
keeps the highest `id` per context, so a re-run (which mints a new, higher check-run id) always wins over
the attempt it replaced — no stale failure latched, and equally no stale success: a tier re-run that is
back `in_progress` reads as non-terminal and the aggregator keeps waiting. The job summary records the
observed `(context, check_run_id, status, conclusion, run URL)` tuple for every registered tier, so the
evidence for a green `required` is inspectable after the fact.

The one residual: a tier re-run *after* `required` has already gone green cannot be retracted by a finished
job. Doctrine therefore states the re-run order — re-run the tier, then re-run `required` — and
`scripts/flow/premerge-assert.sh` remains the closer's last look.

**Interaction with `--auto`.** Arming happens before tiers finish; that stays true and is fine, because
GitHub releases the merge on the `required` context turning green, and under this change `required` cannot
turn green until every registered tier has already reported success. Workers keep arming immediately
(#2667) and still never poll a PR's own CI.

**Break-glass.** A GitHub-side incident can leave a registered tier permanently absent. The escape hatch is
a per-tier label `ci:waive:<tier-id>` — never a blanket bypass — honoured **only** for an absent or
non-terminal tier and **never** for a `failure`/`cancelled`/`timed_out` conclusion. Each waiver emits a
`::warning::` annotation and a job-summary line naming the tier and the actor. It is an owner action; a
worker never needs it, which preserves the "no per-PR knowledge" acceptance criterion.

## Testability, and why it must be provably-failing

`aggregate-required-tiers.sh` takes its check-run JSON, its registry path, and its deadline/poll budget
from injectable inputs, so the whole decision surface runs offline against synthetic fixtures with no
network and no sleeping. Tests assert on **observed check-run states**, never on elapsed wall-clock
(#2642) — expiry is exercised by injecting an already-passed deadline or a zero poll budget.

A guard that cannot fail is the recurring defect in this codebase (five found in adjacent work this week),
so non-vacuity is an acceptance criterion, not a hope: substituting a stub aggregator that always exits 0
must turn the suite RED, and likewise a stub policy rule must red the `validate-workflows.rb` tests. Every
state gets a discriminating case that proves `required` FAILs when it should.

## Rejected alternatives

- **Add tier contexts to `required_status_checks.contexts`** (#2910 option 2) — the `pr-gate.yml:27-34`
  deadlock trap: a path-filtered context that never fires blocks every affected PR forever, and each added
  context is one more thing branch protection must be edited to change.
- **Pull every wiring-evidence test into `agent-gate.sh`** (#2910 option 3) — does not generalise (the
  local gate cannot host Docker/matrix tiers) and leaves the topology gap intact; per-tier promotion stays
  a #1269 question.
- **Document the gap, require hand-verification** (#2910 option 4) — the current de-facto state, which
  already nearly failed.
- **A `workflow_run`-triggered roll-up that reports a second context** — needs a second required context
  (branch-protection change) and inherits the never-fires trap for filtered producers.

## What behaves differently at the MOMENT OF MERGE (review round 3)

Everything above reasons about steady state. The deployment axis is separate, and it is the axis on which
a CI change lands on everyone at once.

**The split this change created.** Round 2 moved the registry and the aggregator to the pull request's
**base ref** — necessary, or the check is defined by the thing it checks. But the *emitter* (the tier's
gate job) comes from the tree the event ran. Those are two different trees, and at the instant this merges
they can disagree: `main` starts registering `Flight tier gate` while some other tree does not emit it.

Three things bound the blast radius, and they should be stated rather than assumed:

1. **For a `pull_request` event GitHub takes workflow definitions from the MERGE COMMIT, not the head.**
   An open pull request that does not touch `flight-ci.yml` therefore picks up the new tier — trigger,
   classifier, gate job — on its next event, with no rebase. The "every open PR wedges" reading assumes
   head-ref semantics and overstates the risk.
2. **An unrebased head reds `pr-gate-core` first anyway.** `validate-workflows.rb` runs there against the
   head tree and cannot find `.github/ci-gating-tiers.yml`, so `required` fails on the core result long
   before any tier polling. Wrong-but-fast, and never an hour of silence.
3. **The residual is real and is not the unrebased branch.** It is a pull request that RENAMES a registered
   tier's context (or deletes its workflow) *and* updates the registry in the same commit. That head is
   self-consistent, so `pr-gate-core` passes; `required` is evaluating the BASE registry, which still names
   the old context; the context never appears; the aggregator polls for the full hour. Any follow-up change
   to this mechanism has exactly that shape.

**So detection is by construction, not by procedure** (`scripts/ci/gating_head_emitability.rb`): the
aggregating job checks out `github.sha` — for a pull-request event the merge commit, i.e. precisely the
tree GitHub took this run's workflow definitions from — sparse, read-only, no credentials, and only ever
*parsed*. If the base registers a tier that tree provably cannot emit, `required` fails on the FIRST poll
with a diagnostic naming both remedies (rebase, or `ci:waive:<tier-id>` for a deliberate rename). Losing
that checkout is `continue-on-error`: it warns and falls back to ordinary polling, because a false red is
also an outage.

Two directions are deliberately closed:

- **It can only ever fail.** A pull request controls that tree, so "cannot emit, therefore pass" would be a
  one-line bypass: break your own tier workflow, go green.
- **No short absent-deadline.** The obvious "red it in 15 minutes instead of 60" is wrong here: a tier's
  gate job `needs:` every other job in its workflow, so GitHub does not create its check run until they
  finish. Absence for tens of minutes is the NORMAL state of a tier that is genuinely running, and a timer
  on it would red exactly the pull requests that mandate the tier. Speed comes from a provable property
  evaluated immediately, not from a clock.

**Other merge-time deltas, for the record.** (a) The `ci-gating-tiers.yml` registry only starts governing a
pull request once it is on that PR's base — so this change governs nothing until it merges, and everything
one event later. (b) The bootstrap fallback ("the base carries no registry") is reachable only until this
merges; afterwards it is dead code retained for branches cut from an older base, and it cannot be used to
escape a base registry that exists. (c) Re-running an OLD run replays the old event payload and the old
workflow files, so it neither gains nor is broken by this change. (d) From the merge onward, a
`cqlite-core/**` diff mandates the whole Flight tier — a real, intended increase in per-PR CI time that the
registry's `mandate_paths` documents.
