## ADDED Requirements

### Requirement: A CI lane runs the authoritative gate and reds on a broken gate component
A GitHub Actions lane SHALL invoke the full `scripts/agent-gate.sh` (not a `--only` PARTIAL subset) so
that a change which breaks any gate component fails CI rather than merging silently. A deliberately
broken component (e.g. a `node-bindings` regression introduced in the gate) SHALL cause this lane to
report a non-zero / failing check.

#### Scenario: Breaking a gate component reds the lane
- **WHEN** a change makes a gate component fail (e.g. the gate's `node-bindings` step breaks)
- **THEN** the gate CI lane runs `scripts/agent-gate.sh` and reports a failing check
- **AND** the gate's run is a full run (its summary is `RESULT: PASS`/`FAIL`, never a `PARTIAL (--only …)` run that does not count)

#### Scenario: A green tree passes the lane
- **WHEN** the gate CI lane runs against an unbroken tree with datasets present
- **THEN** `scripts/agent-gate.sh` reports `RESULT: PASS` and the check is green

### Requirement: The gate lane is triggered by changes to gate-defining inputs
The PR-triggered gate lane SHALL run when the gate itself or its binding inputs change — at minimum
`scripts/agent-gate.sh` and `bindings/**` — and SHALL NOT be triggered by unrelated documentation-only
changes. This scopes the heavy full-gate run to the PRs that can actually break a gate component, rather
than every `cqlite-core/**` PR.

#### Scenario: Editing the gate script triggers the lane
- **WHEN** a PR modifies `scripts/agent-gate.sh`
- **THEN** the gate CI lane is triggered for that PR

#### Scenario: A docs-only change does not trigger the gate lane
- **WHEN** a PR modifies only documentation outside the lane's path set
- **THEN** the gate CI lane is not triggered

### Requirement: A path-independent full-gate backstop runs on a schedule
A scheduled (cron) run SHALL execute the full `scripts/agent-gate.sh` independent of file paths, as a
backstop that catches gate-component breakage not covered by the PR path filter, and SHALL also be
runnable on demand via `workflow_dispatch`. A failing backstop run SHALL surface as a failed workflow
run.

#### Scenario: The nightly backstop runs the full gate
- **WHEN** the scheduled trigger fires (or the workflow is dispatched manually)
- **THEN** the full gate runs path-independently
- **AND** a component failure makes the scheduled run fail (visible on the Actions dashboard)

### Requirement: The gate lane provisions datasets so dataset-dependent components do not skip
The gate lane SHALL fetch the pinned test datasets and set `CQLITE_DATASETS_ROOT` before running the
gate, so the dataset-dependent components (core/integration/write/smoke/tombstones-scan) execute against
real SSTables rather than silently skipping. A dataset-dependent test that finds 0 rows when data is
present SHALL remain a failure (existing doctrine), not a skip.

#### Scenario: Dataset-dependent components execute in the lane
- **WHEN** the gate lane runs
- **THEN** the datasets are fetched and `CQLITE_DATASETS_ROOT` is set so dataset-dependent gate components run (not skip)
