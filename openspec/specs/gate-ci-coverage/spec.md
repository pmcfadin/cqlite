# gate-ci-coverage Specification

## Purpose
TBD - created by archiving change gate-ci-coverage. Update Purpose after archive.
## Requirements
### Requirement: A CI lane runs the authoritative gate and reds on a broken gate component
A GitHub Actions lane SHALL invoke the full `scripts/agent-gate.sh` (not a `--only` PARTIAL subset) so
that a change which breaks any gate component fails a CI run rather than going unverified. This lane is
the nightly/scheduled + `workflow_dispatch` deep-check run (it is NOT a required per-PR check). A
deliberately broken component (e.g. a `node-bindings` regression introduced in the gate) SHALL cause a
scheduled or dispatched run of this lane to report a non-zero / failing run.

#### Scenario: Breaking a gate component reds the scheduled/dispatch run
- **WHEN** a change makes a gate component fail (e.g. the gate's `node-bindings` step breaks)
- **THEN** the scheduled or `workflow_dispatch` gate run executes `scripts/agent-gate.sh` and reports a failing run
- **AND** the gate's run is a full run (its summary is `RESULT: PASS`/`FAIL`, never a `PARTIAL (--only …)` run that does not count)

#### Scenario: A green tree passes the lane
- **WHEN** the gate CI lane runs against an unbroken tree with datasets present
- **THEN** `scripts/agent-gate.sh` reports `RESULT: PASS` and the run is green

### Requirement: The full gate runs path-independently on a nightly schedule and on demand, and is not a required per-PR check
The full `scripts/agent-gate.sh` SHALL run as a **path-independent nightly deep-check backstop** via a
`schedule:` cron and SHALL also be runnable **on demand** via `workflow_dispatch`. This lane SHALL NOT be
a required per-PR check and SHALL NOT carry a `pull_request` trigger, so it does not duplicate or
contradict the light, always-running required PR check `.github/workflows/pr-gate.yml` established by
epic #1360 (PR #1377). The nightly run is the path-independent backstop that catches gate-component
breakage a light PR check cannot see; a failing run SHALL surface as a failed workflow run on the Actions
dashboard.

#### Scenario: The nightly schedule runs the full gate regardless of file paths
- **WHEN** the `schedule:` cron fires
- **THEN** the full gate runs path-independently (no `paths:` filter gates it)
- **AND** a component failure makes the scheduled run fail (visible on the Actions dashboard)

#### Scenario: The gate runs on demand via workflow_dispatch
- **WHEN** the workflow is dispatched manually
- **THEN** the full gate runs on demand and its result is reported

#### Scenario: The gate lane is not a required per-PR check
- **WHEN** any pull request is opened
- **THEN** this lane does not run for the PR (it has no `pull_request` trigger), so it neither duplicates nor blocks the required light `pr-gate.yml` check

### Requirement: The gate lane provisions datasets so dataset-dependent components do not skip
The gate lane SHALL fetch the pinned test datasets and set `CQLITE_DATASETS_ROOT` before running the
gate, so the dataset-dependent components (core/integration/write/smoke/tombstones-scan) execute against
real SSTables rather than silently skipping. A dataset-dependent test that finds 0 rows when data is
present SHALL remain a failure (existing doctrine), not a skip.

#### Scenario: Dataset-dependent components execute in the lane
- **WHEN** the gate lane runs
- **THEN** the datasets are fetched and `CQLITE_DATASETS_ROOT` is set so dataset-dependent gate components run (not skip)

