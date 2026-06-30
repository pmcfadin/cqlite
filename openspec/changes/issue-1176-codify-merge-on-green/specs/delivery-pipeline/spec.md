## ADDED Requirements

### Requirement: Workers land green PRs via merge-on-green and never busy-wait on external CI
A flow-lead worker SHALL treat **PR-open + `agent-gate.sh` PASS + roborev clean (+ spec-auditor C PASS for
design-driven changes)** as its terminal state for an issue, arm a merge-on-green mechanism, and end its
turn. It SHALL NOT poll the PR's external CI in a yield/wake loop (e.g. repeated `ScheduleWakeup` cycles)
after the work is complete. The merge-on-green mechanism SHALL be `gh pr merge --auto --squash
--delete-branch` where branch-protection auto-merge is available, and a manager-owned poller/merge-engine
otherwise; the chosen path SHALL be logged. Merge-on-green SHALL only land a PR once a defined green
signal exists (configured required checks, or the manager-poller's explicit lane set) — it SHALL NOT
auto-land against an empty required-check set.

#### Scenario: Worker opens the PR and stops without CI-poll cycles
- **WHEN** a worker reaches PR-open with gate PASS + roborev clean (+ C PASS for design-driven)
- **THEN** it arms the merge-on-green mechanism and ends its turn
- **AND** it does not schedule repeated wake-ups to poll that PR's external CI (no N stop/resume CI-poll cycles)

#### Scenario: Auto-merge available → native merge-on-green
- **WHEN** branch-protection auto-merge is enabled for the repo
- **THEN** the worker arms `gh pr merge --auto --squash --delete-branch` and logs that path
- **AND** GitHub lands the PR when the defined required checks pass, auto-closing the issue via `Closes #N`

#### Scenario: Auto-merge unavailable → manager-poller fallback
- **WHEN** branch-protection auto-merge is not available
- **THEN** the worker hands off to the manager-owned poller/merge-engine and logs that path
- **AND** the PR is landed on green by that mechanism rather than by the worker polling CI

#### Scenario: No defined green signal → do not auto-land prematurely
- **WHEN** the PR's branch has no configured required status checks
- **THEN** the merge-on-green mechanism does not immediately merge on an empty check set
- **AND** it lands only once the chosen green signal (configured required checks or the poller's explicit lane set) is satisfied

### Requirement: Delivery doctrine forbids worker CI busy-waiting
The delivery doctrine SHALL document merge-on-green and explicitly forbid worker CI busy-waiting.
`docs/development/pm-operating-loop.md` and the `agents-developing/delivery-pipeline` page SHALL describe
the worker terminal state (PR-open + green quality bar → arm merge-on-green → stop) and state that polling
a PR's own external CI in a yield loop is prohibited; the `worker`/`flow-implement` skill text SHALL be
consistent with this.

#### Scenario: Doctrine and skills describe merge-on-green and prohibit busy-wait
- **WHEN** `pm-operating-loop.md`, the delivery-pipeline page, and the worker/flow-implement skill text are read after this change
- **THEN** they describe the worker terminal state + the merge-on-green mechanism (native `--auto` preferred, manager-poller fallback)
- **AND** they explicitly state that a worker must not busy-poll its PR's external CI after the work is complete
