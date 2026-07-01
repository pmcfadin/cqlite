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

### Migration Rename

The required check set will be renamed to `Required PR Gate / required` in
issue #1364 after the workflow exists and has been proven on a PR.

The current checked-in branch-protection config still contains these legacy
contexts to avoid requiring a missing check during Wave 1:

- `CI / test`
- `CI: Core Library (minimal) / m1-core-validation`
- `CI: Core Library (minimal) / sstabledump-parity-m1`
- `CI: SSTableDump Parity Gate / sstabledump-parity`

Those contexts should be retired from global branch protection when #1364
updates `.github/branch-protection.json` to the new aggregate. They may continue
to exist as targeted, nightly, or release validation, but they should not be
added back as global branch-protection requirements after the migration.

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
`compression-corruption-parity.yml`, `cql-type-parity.yml`,
`live-cell-compaction-parity.yml`, `tombstone-ttl-parity.yml`).

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
| Publish dry-runs | `trino-publish.yml` manual `dry_run=true`; `node-release.yml` `npm pack --dry-run` step; `python-release.yml` `twine check dist/*` step; local CLI archive build matching `release.yml` | Dry-run or metadata validation output captured before real publish |
| Actual publish workflows | `release.yml`, `node-release.yml`, `python-release.yml`, `trino-publish.yml` | Run only from intentional release tags or explicit maintainer dispatch; never from schedules |

Release checks are release gates, not global PR branch-protection checks. No
release or publish workflow may be added to `schedule`, and nightly validation
must not upload packages, images, Maven artifacts, npm packages, Python
distributions, or GitHub release assets.

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
