# CI Runtime Dashboard

This dashboard is the operational record for epic #1360. It tracks whether the
CI runtime overhaul is actually reducing ordinary PR latency while preserving
the deep nightly and release backstops.

## Instrumented Workflows

The runtime helper is `scripts/ci/ci-timing-summary.sh`. It records TSV rows in
`$CI_TIMING_FILE` or `$RUNNER_TEMP/ci-timing.tsv` and appends a Markdown table
to `$GITHUB_STEP_SUMMARY`.

Current instrumentation:

| Workflow | Timing coverage |
| --- | --- |
| `pr-gate.yml` | Workflow policy, `cargo fmt`, hard `cqlite-core` Clippy, all-feature build, fast tests |
| `ci.yml` | Dataset fetches, core lib/doc tests, nextest archive, nextest partitions, integration groups, write/delta groups, CLI smoke, transitional required context |
| `sstabledump-parity-gate.yml` | Dataset restore, provenance, diagnostics, smoke parity groups, full parity groups |

## Baseline And Targets

Capture the baseline from the first 20 PR runs on this branch before changing
branch protection to require `Required PR Gate / required`. Targets are p95
unless a row says otherwise.

| Metric | Baseline source | Target |
| --- | --- | --- |
| Required PR queue-to-green | First 20 `Required PR Gate` pull-request runs after merge | p50 <= 8 min, p95 <= 20 min, max non-infra run <= 30 min |
| Required workflow fan-out per ordinary PR | First 20 ordinary pull requests without full-tier labels | Docs-only <= 2 workflows; code PR <= 3 default workflows plus path-targeted smoke only |
| Full-tier opt-in fan-out | Labeled PRs using `ci:broad`, `ci:bindings-full`, `ci:docs-full`, `ci:flight-full`, `ci:ingest-full`, `ci:observability-overhead`, `ci:perf`, or `ci:trino-full` | Extra workflows are explainable by label or path and absent from unrelated PRs |
| Cargo/sccache hit rate | Step summaries plus `Show sccache stats` output | Warm PR p95 has >= 70% cache hits; nightly/release lanes do not regress below the prior 7-day median |
| Dataset cache hit rate | Dataset restore/fetch steps and `actions/cache` output | Warm nightly/full parity hit rate >= 85%; a miss must still fail closed if fixture provenance is incomplete |
| Required PR job runtime | `Required PR Gate / required` job timing table | p95 <= 20 min |
| Smoke parity job runtime | `sstabledump-parity` timing table | p95 <= 30 min when smoke scope runs |
| Full parity job runtime | `sstabledump-parity-full` timing table | p95 <= 180 min and under the 240 min timeout |

Do not tighten branch protection around the new required context until the
baseline sample is green and the required PR target is met without manual
reruns.

## Measurement Commands

Recent workflow runs:

```bash
gh run list --limit 20 --json workflowName,event,status,conclusion,createdAt,updatedAt
```

Recent PR fan-out:

```bash
gh run list --event pull_request --limit 100 \
  --json databaseId,workflowName,event,status,conclusion,createdAt,updatedAt,headBranch
```

Job runtimes for one run:

```bash
gh run view <run-id> --json jobs \
  --jq '.jobs[] | {name, conclusion, startedAt, completedAt}'
```

Timing tables and cache evidence:

```bash
gh run view <run-id> --log \
  | rg 'CI runtime timing|Required PR Gate timing|SSTableDump .* timing|cache-hit|sccache'
```

Queue-to-green for a single run is `updatedAt - createdAt` from `gh run list`.
Job runtime is `completedAt - startedAt` from `gh run view --json jobs`.

## Follow-Up Measurement Loop

1. After the first PR run on this branch, save the `gh run list --limit 20`
   output in the epic issue and confirm `Required PR Gate / required` appears.
2. After 20 PR samples, compute p50 and p95 queue-to-green for ordinary PRs.
3. Compare workflow fan-out for docs-only, Rust-only, bindings, and full-tier
   label PRs. Any default full matrix on an unrelated PR is a regression.
4. Review cache hit evidence weekly for the first two weeks. Dataset misses are
   acceptable only when the restore/fetch path still reports provenance and
   fixture counts.
5. Move branch protection from legacy contexts to `Required PR Gate / required`
   only after the required gate is stable on real PRs.

## Nightly Failure Runbook

1. Open the step summary first. The timing table should identify the slow or
   failed dataset, cargo, parity, or matrix group.
2. Download artifacts before rerunning. Parity artifacts, coverage reports, and
   regeneration reports are the source of truth for follow-up issues.
3. Classify the failure as code regression, fixture/provenance drift, upstream
   outage, runner capacity, or flaky test.
4. Rerun manually only after recording the original run URL. If the rerun passes,
   keep both URLs in the issue or release checklist.
5. For parity failures, attach the parity summary and any validation artifacts.
   Do not regenerate or publish datasets from nightly workflows.
6. Treat stale, missing, or artifact-less release-gate nightly runs as red until
   a maintainer explicitly waives them for the release.
