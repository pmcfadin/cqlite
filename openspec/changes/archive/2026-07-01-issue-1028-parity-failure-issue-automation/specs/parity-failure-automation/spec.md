## ADDED Requirements

### Requirement: Scheduled/main parity failures create a deduplicated tracking issue
The automation SHALL, when a manifest-backed parity workflow completes with a `failure` conclusion on a
scheduled, main-branch, or manual (`workflow_dispatch`) run, create a GitHub issue per distinct failing
scenario fingerprint, OR update the existing open issue carrying that fingerprint marker instead of creating a
duplicate. Pull-request-triggered parity failures SHALL NOT create issues (they retain comment-on-PR
behavior). The issue body SHALL include the parent epic reference `#974`, the manifest scenario ID, the CI
tier, artifact links, a reproduction command, and the latest failure summary.

#### Scenario: A scheduled parity failure opens a tracking issue
- **WHEN** a parity lane completes with conclusion `failure` on a `schedule` or main `push` run and no open issue carries that scenario's fingerprint marker
- **THEN** the automation creates one issue labeled `parity-failure` whose body contains `<!-- PARITY-FAIL:<fingerprint> -->`, the parent epic `#974`, the scenario ID, the CI tier, artifact links, a reproduction command, and the latest failure summary

#### Scenario: A repeat failure updates the existing issue, not a duplicate
- **WHEN** a parity lane fails with a fingerprint that matches the marker in an already-open `parity-failure` issue
- **THEN** the automation adds a dated failure comment and refreshes the latest-run link on that existing issue
- **AND** it does NOT create a new issue

#### Scenario: PR-triggered failures do not file issues
- **WHEN** the failing parity run was triggered by a `pull_request`
- **THEN** the automation files no issue (PR comment behavior is unchanged)

### Requirement: The failure fingerprint is stable across runs
The automation SHALL compute each failing scenario's fingerprint as a versioned hash over normalized,
ordered fields — manifest scenario ID, workflow, test target, fixture/component path, and a normalized
failure class — such that the same logical failure produces the same fingerprint across runs, and run-variant
noise (timestamps, counts, absolute paths) does NOT change it. The normalized failure class SHALL use a stable
vocabulary (the `VerifyErrorClass` codes where applicable, else a lane-defined stable code).

#### Scenario: Same logical failure yields the same fingerprint
- **WHEN** the same scenario fails on two different runs with differing timestamps, run IDs, and counts
- **THEN** both runs compute an identical fingerprint
- **AND** the second run updates the first run's issue rather than opening a new one

#### Scenario: Different scenarios or failure classes yield different fingerprints
- **WHEN** two failures differ in scenario ID, workflow, test target, component path, or normalized failure class
- **THEN** their fingerprints differ and they are tracked as separate issues

### Requirement: Failure inputs are read from a structured artifact with a surfaced fallback
The parity lanes SHALL emit a machine-readable `parity-failures.json` listing each failing scenario's
`{scenario_id, workflow, test_target, component_path, failure_class}`, and the automation SHALL compute
fingerprints from it. Where a failed run has no such artifact, the automation SHALL fall back to parsing the
lane's summary/logs and SHALL record in the workflow run summary that it used the degraded path; it SHALL
treat parsing zero failures from a run that concluded `failure` as an anomaly to surface, never a silent no-op.

#### Scenario: Structured artifact drives fingerprinting
- **WHEN** a failed run uploaded `parity-failures.json`
- **THEN** the automation computes fingerprints from its structured fields

#### Scenario: Degraded fallback is surfaced, not silent
- **WHEN** a failed run has no `parity-failures.json`
- **THEN** the automation falls back to parsing the summary/logs AND prints a notice in the run summary that the degraded path was used
- **AND** if it parses zero failures from a `failure`-concluded run, it surfaces that as an anomaly rather than silently filing nothing

### Requirement: Issue filing is non-gating, fails open, and never auto-closes
The automation SHALL run independently of the parity lane's pass/fail and SHALL never change a parity run's
result — issue filing SHALL NOT turn a failing run green nor mask a red one. When the required token is
absent it SHALL emit a `::notice::` and no-op with success (never red CI). It SHALL NOT automatically close
any issue; on a subsequent green run for a tracked fingerprint it SHALL post a resolution comment but leave
closing to a separately-designed green-run policy.

#### Scenario: Missing token no-ops without failing CI
- **WHEN** the issue-write token is absent
- **THEN** the automation emits a `::notice::` and exits successfully without filing or failing CI

#### Scenario: Filing never gates the parity result
- **WHEN** the issue-filing job errors or is skipped
- **THEN** the originating parity run's pass/fail conclusion is unchanged

#### Scenario: A green run does not auto-close
- **WHEN** a previously-failing fingerprint's lane later completes green
- **THEN** the automation posts a resolution comment on the open tracking issue
- **AND** it does NOT automatically close the issue

### Requirement: The automation is recorded as mirrored tooling in the parity manifest
The change SHALL add manifest scenarios under the `cli_reporting` capability recording this automation
(`status: mirrored`, `risk: tooling_only`, `evidence.type: smoke`), modeled on the existing
`cass.cli_reporting.parity_manifest_lint_and_report` scenario, with scenario IDs matching the manifest `id`
pattern; the public parity report SHALL be regenerated and the `cassandra-parity` lint SHALL pass.

#### Scenario: Lint accepts the new tooling scenarios
- **WHEN** `cassandra-parity` lint runs after the manifest additions
- **THEN** the new `cli_reporting` failure-automation scenarios validate with their required tooling fields and the lint passes
- **AND** the regenerated report reflects them
