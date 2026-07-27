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

## Mechanics

**Where the aggregation runs.** `pr-gate.yml` splits into `pr-gate-core` (today's steps, unchanged) and
`required` (`name: required` preserved, `needs: [pr-gate-core]`, `if: always()`), whose only work is
aggregation. Rationale: the required *context name* is unchanged, so branch protection is untouched; the
aggregator gets its own generous `timeout-minutes` without inflating the compute job's; the heavy runner is
released before the wait begins; and `if: always()` guarantees the context still reports when core fails.
`required` fails whenever `needs.pr-gate-core.result != 'success'` — the aggregate can never mask the core.

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
