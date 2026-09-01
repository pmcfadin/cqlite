# CI Tier Policy

This is the source-of-truth policy for CQLite CI tiering during the CI runtime
overhaul. The goal is local-first validation, one small required PR gate, and
deeper validation moved to targeted, nightly, or release workflows.

Runtime targets, baseline capture commands, and the nightly failure runbook live
in `docs/ci/ci-runtime-dashboard.md`.

## Branch Protection Source Of Truth

- `.github/branch-protection.json` is the repository source of truth for
  required status checks.
- `.github/setup-branch-protection.js` must load `.github/branch-protection.json`
  instead of duplicating required check names.
- Do not update `.github/branch-protection.json` to require the new aggregate
  check until `.github/workflows/pr-gate.yml` exists and the check has been
  proven on a PR.

## Globally Required Checks

Only stable aggregate checks may be globally required by branch protection.
Path-filtered workflows, heavyweight matrices, Docker parity jobs, performance
jobs, coverage jobs, language binding matrices, and release workflows must not
be globally required for every pull request.

Target global required status check:

| Check name | Owner | Status |
| --- | --- | --- |
| `Required PR Gate / required` | `.github/workflows/pr-gate.yml` | Planned stable aggregate check for issue #1364 |

Stable aggregate means:

- The workflow runs on every `pull_request` without `paths` or `paths-ignore`
  filters.
- The required job is an aggregate job with a stable display name.
- The aggregate fails closed when any required internal validation fails.
- The aggregate remains small enough for ordinary PR feedback.

### Migration Complete (issue #2648)

The migration to a single required check is complete. Live branch protection
requires exactly one status check — `required`, produced by the `required` job
in `.github/workflows/pr-gate.yml` — with `strict = false`. Issue #2648
reconciled the checked-in `.github/branch-protection.json` to that live truth
(single `required` context, `strict: false`), so re-running
`setup-branch-protection.js` can no longer restore the retired legacy contexts:

- `CI / test`
- `CI: Core Library (minimal) / m1-core-validation`
- `CI: Core Library (minimal) / sstabledump-parity-m1`
- `CI: SSTableDump Parity Gate / sstabledump-parity`

These are retired from global branch protection. The `m1-ci.yml` stub (which
kept the two `m1` contexts emitted) has been deleted, and the transitional
compile shim inside `CI / test` was removed while the `test` job is retained as
a non-required broad-CI aggregator. These contexts may continue to exist as
targeted, nightly, or release validation, but they must not be re-added as
global branch-protection requirements.

## Tier Contracts

### Local Pre-Merge

Purpose: the primary engineering signal before opening or updating a PR.

Default command:

```bash
bash scripts/local/pre-merge.sh fast
```

Required local coverage:

- Formatting.
- `cargo clippy --package cqlite-core --all-targets --all-features -- -D warnings`.
- `cqlite-core` all-feature build.
- Fast representative tests for the changed surface.
- Change-specific deeper checks selected by the author.

Use `core` for additional doctest and M1 parser smoke coverage, `storage` for
dataset provenance plus focused SSTable parity smoke, `bindings` on Linux hosts
with local Python/Node toolchains, and `full` for the broadest local pre-merge
pass. Optional checks must print exact follow-up commands when skipped; required
checks fail closed.

Local checks are not GitHub branch-protection contexts. They should be runnable
from a maintained local entrypoint after issue #1362.

### Required PR

Purpose: one light, always-running GitHub Actions check that confirms the basic
merge contract.

Allowed contents:

- Workflow/config validation.
- Rust formatting.
- `cargo clippy --package cqlite-core --all-targets --all-features -- -D warnings`.
- `cqlite-core` all-feature build.
- Fast representative tests.

Not allowed:

- Docker.
- Full dataset downloads.
- Full SSTableDump parity.
- Coverage enforcement.
- Performance benchmarking.
- Node, Python, Flight, Trino, or OS/architecture matrices.
- Any path filter that can skip the required check on a docs-only PR.

The target globally required status check for this tier is
`Required PR Gate / required`.

#### The sole required context AGGREGATES sibling tiers (issue #2910)

`required` is no longer only its own steps. `pr-gate.yml` is split into
`pr-gate-core` (the light contents above) and `required` (`needs: [pr-gate-core]`,
`if: always()`, the unchanged branch-protection context name), whose additional
job is to poll the pull request head's sibling check runs and **fail closed** on
any tier declared in `.github/ci-gating-tiers.yml` that is failed, still
non-terminal at the aggregation deadline, or **absent**. It never masks the core:
`pr-gate-core` not concluding `success` fails `required` regardless of tier state.

Rules for `pull_request`-triggered workflows:

- Every one must be listed in `.github/ci-gating-tiers.yml`, either under
  `tiers:` (it gates the merge) or under `exempt:` with a `reason` and an `issue`.
  This is enforced by `scripts/ci/validate-workflows.rb`, which runs as a step in
  `pr-gate-core`; `required` needs that job and fails unconditionally unless it
  concluded `success`, so forgetting to enrol reds `required`. (Of the 25
  PR-triggered workflows today, one is the aggregator, TWO are gating tiers —
  `flight-ci.yml` and, since #3640, `node-ci.yml` on all three test platforms —
  and 22 are exempted.)
- A workflow under `tiers:` must ALWAYS fire — no blocking trigger `paths:` /
  `paths-ignore:` / `branches:` (only the
  `__required_ci_context_never_matches__` sentinel used by `ci.yml`), and its
  `pull_request.types` must include every event that mints a new head sha
  (`opened`, `synchronize`) while staying within the aggregator's own observed
  set. Applicability moves into a cheap, unconditional classifier job following
  the `observability-gate.yml` `classify` pattern, and the tier's expensive jobs
  stay gated on the classifier's output.
- A workflow under `tiers:` must emit its declared context from exactly one gate
  job whose condition is exactly `if: ${{ !cancelled() }}`, for each of whose
  dependencies some step both reads `needs.<job>.result` and can exit non-zero,
  and whose `needs` closure covers every other job in the workflow.
  Inapplicability is reported as an explicit SUCCESS from that job — never as an
  absent check run. A bare `always()` is **rejected**: it runs the gate job while
  the run is *being cancelled*, when every `needs.*.result` is `cancelled`, and
  the gate turns that into a `failure` conclusion — which makes the aggregator's
  supersession grace unreachable, so every routine supersession would red
  `required`.
- **Migration states red fast, not after an hour.** The registry is read from the
  base ref while the emitter comes from the tree the event ran (for a
  `pull_request` event, the merge commit). If the base registers a tier whose
  context that tree provably cannot emit — workflow absent, no PR trigger,
  `types:`/`branches:` excluding this event, or no job with that name — `required`
  fails on the first poll and names the remedy: rebase, or `ci:waive:<tier-id>` if
  the tier is deliberately being renamed or retired (a registry change only takes
  effect once merged). Inconclusive evidence never produces that verdict, and the
  verdict is never a pass.
- `required` must itself fire on `labeled`/`unlabeled`, or the `ci:waive:<tier-id>`
  break-glass could never be exercised on a PR the mechanism has wedged.
- Failing closed applies at the DEADLINE, not to every transient. A `cancelled`
  tier (routine under `cancel-in-progress`) is re-polled while a replacement run
  is plausible and fails once the grace lapses; a transport failure reading the
  check-runs API is retried and fails only on persistence.
- For a registered tier, a **diff-based mandate overrides the `ci:*` label**: a
  mandating diff runs the tier with or without the label; the label stays an
  opt-in only for non-mandating diffs.
- The aggregation deadline (registry `wait_minutes`, max over tiers) must be
  strictly less than the `required` job's `timeout-minutes`, so expiry is a
  reported red with a diagnostic rather than an Actions cancellation.
- Break-glass is per-tier only: `ci:waive:<tier-id>` excuses an absent or pending
  tier, never a failed one. There is no blanket waiver. It must not fight the
  tier it waives: a registered tier may NOT cancel its in-flight run on a label
  event (the enrolment rule rejects any `cancel-in-progress` that is not provably
  false for `labeled`/`unlabeled` — the literal `true` AND the near-miss
  `${{ github.event_name == 'pull_request' }}`, which is true for label events),
  and a pending tier whose only check run was minted at/after the waiver was
  applied resolves at once instead of waiting out the deadline.
- The honoured-waiver annotation names WHO APPLIED THE LABEL, resolved from the
  PR's `labeled` events — not the actor of the run, who is usually someone else.
  An unresolvable attribution says so rather than naming anyone.
- The waiver-evidence report states OBSERVATIONS, never a derived capability. It
  claims that no waiver label is present only when the LIVE label read succeeded on
  that poll; a read that fell back to the run-start event payload reports UNKNOWN
  instead, because a label applied mid-run is invisible to that snapshot. `labeled`
  events are immutable history, so evidence is counted only for a waiver label
  actually IN FORCE, and an untrusted label read makes that count UNKNOWN rather
  than zero. Whether such an event BINDS stays a per-tier verdict. Every one of
  those states is reported and none of them changes the verdict.
- No two lines of one report may contradict each other. Every claim about labels is
  phrased against "the label set this run is using" plus that set's provenance (live
  read, or the run-start payload), because that is the only thing the report observes;
  and a run that ADMITS a read failed emits no denial that one did — the operator never
  gets `permissions:` advice beside an "authorization is not the problem" line.
- A waiver label is `ci:waive:<tier-id>` with a LOWER-CASE tier id
  (`[a-z0-9][a-z0-9-]*`) — the shape the evaluator matches. A `ci:waive:`-prefixed
  label that misses it (`ci:waive:Flight`) waives nothing, and the report says so,
  naming the label: it is neither a waiver in force (nothing is read for it, and no
  evidence state is reported about it) nor an absence of waiver labels.
- A registered tier's context is satisfied ONLY by a check run GitHub Actions
  produced. A check-run name is global to the commit and anything with
  `checks:write` can mint one, so provenance (`app` + an Actions run URL) is
  verified fail-closed; an unverifiable run neither satisfies nor shadows the
  genuine one.
- A tier's gate job must VALIDATE its classifier's applicability verdict.
  `skipped` counts as a pass only because an inapplicable tier reports itself
  that way; an empty or non-boolean verdict reds the tier rather than reading as
  "not applicable", and a verdict of "applies" with skipped work reds it too.
- The gating scripts are ruby-only and declare their interpreter floor
  (`scripts/ci/gating_ruby_floor.rb`, ruby >= 3.0) in one place; below it the
  aggregation fails closed with the remedy and the self-tests SKIP with the
  reason instead of mis-running (macOS system ruby is 2.6).

`.github/branch-protection.json` keeps `contexts: ["required"]` — issue #2910 adds
no context. It does reconcile the file's review block to the live, intended
policy (`required_approving_review_count: 0`, `require_code_owner_reviews: false`,
`require_last_push_approval: false`): the file is applied verbatim, so an
aspirational value there would switch `main` to a policy the autonomous
merge-on-green pipeline cannot satisfy. Enforcement is the single `required`
context plus `enforce_admins: true`; `.github/CODEOWNERS` is an advisory review
request on `/.github/` and `/scripts/ci/` diffs, not a merge control. See
`.github/QUALITY_GATES_ENFORCEMENT.md`.

### Targeted And Nightly

Purpose: run expensive or surface-specific validation without blocking unrelated
ordinary PRs.

Allowed triggers:

- Path filters.
- Labels.
- `workflow_dispatch`.
- `schedule`.
- Pushes to `main` where appropriate.

Typical checks:

- SSTableDump parity and Cassandra ingest validation.
- Minimal-feature matrices.
- Coverage and performance gates.
- Observability overhead checks.
- Node, Python, Flight, Trino, docs, and packaging smoke checks.

Nightly deep validation must be manually rerunnable with `workflow_dispatch`.
Nightly runs are staggered so slow Cassandra, dataset, coverage, and matrix jobs
do not all start on the same minute:

| Workflow | Nightly UTC offset | Manual rerun | Primary evidence |
| --- | ---: | --- | --- |
| `.github/workflows/quality-gates.yml` | 02:05 | `workflow_dispatch` | `quality-gate-report-*` artifact and step summary |
| `.github/workflows/coverage-baseline.yml` | 02:45 | `workflow_dispatch` | `coverage-baseline-*` Cobertura artifact and step summary |
| `.github/workflows/coverage.yml` | 07:35 | `workflow_dispatch` | `coverage-*-reports` artifacts and coverage summaries |
| `.github/workflows/exhaustive-regeneration.yml` | 08:10 | `workflow_dispatch` | `exhaustive-regeneration-report` and `parity-failures` artifacts |

The existing surface-specific nightly lanes remain part of the deep backstop:
bindings (`node-ci.yml`, `python-ci.yml`), Flight/Trino
(`flight-ci.yml`, `flight-trino-e2e.yml`, `trino-connector-ci.yml`),
performance (`perf-regression.yml`), observability (`observability-gate.yml`),
and parity/ingest lanes (`nightly-docker-parity.yml`,
`sstabledump-parity-gate.yml`, `e2e-readback.yml`, `compaction-parity.yml`,
`parity-regen-matrix.yml`, `live-cell-compaction-parity.yml`).

These workflows can be required by release process policy or by maintainers for
specific PRs, but they must not be globally required in branch protection.

### Nightly Failure Response

Nightly failures are release-blocking until triaged or explicitly waived by the
release owner in the release notes. Maintainers should:

1. Open the workflow step summary first and identify the failing job, matrix leg,
   dataset, or parity scenario.
2. Download retained artifacts before rerunning. Coverage and quality artifacts
   are retained for at least 30 days; exhaustive regeneration artifacts are
   retained for 90 days.
3. Classify the failure as code regression, fixture/provenance drift, upstream
   service outage, runner capacity, or known flaky test.
4. Rerun manually only after capturing the first-run artifact link. If the rerun
   passes, record both run links in the issue or release checklist.
5. For parity failures, attach `parity-failures` and the lane report artifact to
   the follow-up issue. Do not regenerate or publish datasets from a nightly run.
6. Keep releases blocked when any required release-gate lane is red, stale, or
   missing artifacts.

### Release

Purpose: prove release readiness.

Required release coverage must be collected before pushing a `v*` release tag or
running any publish workflow with a non-dry-run mode:

| Gate | Workflow evidence | Required result |
| --- | --- | --- |
| Core/full Rust matrix | `.github/workflows/ci.yml` with `workflow_dispatch` `scope=broad` | Green run on the release commit |
| Full parity and corpus audit | `nightly-docker-parity.yml`, `sstabledump-parity-gate.yml`, `exhaustive-regeneration.yml`, and the focused parity lanes listed above | Green run within the release window, with retained artifacts |
| Cassandra ingest/readback | `cassandra-validation.yml` manual run and `e2e-readback.yml` nightly/manual run | Green run, or documented external outage with rerun scheduled |
| Coverage | `coverage.yml` and `coverage-baseline.yml` | Green run with downloadable reports |
| Performance | `perf-regression.yml` nightly/manual run | No regression beyond policy threshold, or release-owner waiver |
| Supported binding and integration matrices | `node-ci.yml`, `python-ci.yml`, `flight-ci.yml`, `flight-trino-e2e.yml`, `trino-connector-ci.yml`, and `docs-site.yml` where applicable | Green full/nightly/manual matrix for changed release surfaces |
| Publish dry-runs | `trino-publish.yml` manual dispatch (`dry_run` **defaults to `true`**, #2639); `node-release.yml` `npm pack --dry-run` step; `python-release.yml` `twine check dist/*` step; local CLI archive build matching `release.yml` | Dry-run or metadata validation output captured before real publish |
| Actual publish workflows | `release.yml`, `node-release.yml`, `python-release.yml`, `trino-publish.yml` | Run only from intentional release tags or explicit maintainer dispatch; never from schedules |

Release checks are release gates, not global PR branch-protection checks. No
release or publish workflow may be added to `schedule`, and nightly validation
must not upload packages, images, Maven artifacts, npm packages, Python
distributions, or GitHub release assets.

#### Armed-publish dispatch guards (issue #2639)

A bare `workflow_dispatch` on a publishing workflow must never publish or move a
release tag by accident. Two fail-closed guards enforce this; both are covered by
`scripts/ci/validate-workflows.rb` so they cannot silently regress out of the
workflow files:

- **`trino-publish.yml` — `dry_run` defaults to `true`.** `gh workflow run
  trino-publish.yml -f version=X` (no `dry_run`) does a *local-only*
  `publishToMavenLocal` — it never reaches Maven Central. A real Central release
  requires an explicit `-f dry_run=false` (still gated on the secrets check). A
  `v*` tag push is unaffected: `dry_run` applies only to `workflow_dispatch`.
- **`flight-image.yml` — release-tag provenance.** A manual `version` dispatch can
  build from an arbitrary ref. Before any `vX.Y.Z` / `vX.Y` / `latest` tag is
  applied, the `merge` job asserts that `refs/tags/v$version` already resolves to
  the exact commit this run is building (`github.sha`) and refuses (`exit 1`)
  otherwise. A dispatch may therefore only *republish an existing release tag's
  commit* — it can never mint or move a release tag for an arbitrary ref. Push the
  tag first (`git push origin vX.Y.Z`), or use the one-off `image_tag` input for a
  non-release image. This is the PRIMARY guard (provenance, not a prompt).

If a release gate is waived, the waiver must name the failed workflow, run URL,
artifact reviewed, reason for proceeding, and follow-up issue.

## Quality-Gates Workflow Contract

`.github/workflows/quality-gates.yml` is a nightly and manually runnable
coordination workflow during this migration. It must not reference missing
workflows. If it delegates to another workflow in the future, that workflow must
exist and expose `workflow_call`.

This workflow is not the globally required PR gate unless it is explicitly
changed to satisfy the Required PR tier contract above and branch protection is
updated to its aggregate status name.
