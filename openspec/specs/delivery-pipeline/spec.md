# delivery-pipeline Specification

## Purpose
TBD - created by archiving change flow-lead-orchestration. Update Purpose after archive.
## Requirements
### Requirement: Manager agent orchestrates, never implements
The workflow SHALL provide a `flow-lead` manager agent, set as the session
default agent, that orients from the board on start and drives the delivery
pipeline. `flow-lead` SHALL spawn and sequence specialist agents for all
implementation and review work and SHALL NOT write production code itself.

#### Scenario: Orients from the board on start
- **WHEN** a session starts as `flow-lead`
- **THEN** it derives state from open GitHub issues (by `status:*` label + `P?` priority) and open `issue-*` PRs/CI
- **AND** it surfaces the single furthest-along item waiting on the owner (or a short pick-list when nothing is waiting)

#### Scenario: Delegates the middle
- **WHEN** implementation or review work is required
- **THEN** `flow-lead` spawns the relevant specialist agent(s) and does not edit production code directly

### Requirement: Defined pipeline verbs
The workflow SHALL provide `flow-*` skills covering groom → activate →
implement → address → finalize, plus a board/visibility skill. Each verb SHALL
wrap the existing tools (OpenSpec, `agent-gate.sh`, C, roborev), not replace
them.

#### Scenario: Groom produces one scoped issue
- **WHEN** the owner grooms a rough idea
- **THEN** the result is exactly one GitHub issue with one `P0`–`P3` label, `status:ready`, and testable acceptance criteria

#### Scenario: Activate stops at the spec-approval seam
- **WHEN** an issue is activated
- **THEN** a worktree + branch + OpenSpec change are created and the spec + recommended design are rendered inline
- **AND** the pipeline STOPS for the owner's approval (Seam 1) and does not implement

#### Scenario: Implement builds and opens a PR via the quality stages
- **WHEN** `flow-implement` runs on an owner-approved issue
- **THEN** the specialist team builds in the issue's worktree, then `agent-gate.sh`, the C intent audit, and roborev run
- **AND** a PR is opened (and, by default, NOT merged)

### Requirement: Autonomy and human seams (pre-authorized merge-on-green)
By default the lead SHALL open a PR but SHALL NOT merge or close it; merge is
the owner's seam. The lead MAY squash-merge and finalize ONLY a set the owner
has explicitly pre-authorized, and only when `agent-gate` PASS, C PASS, and
roborev clean all hold. Product decisions, scope/title changes, and epic
closes SHALL always be escalated to the owner (a NEEDS-YOU list), never decided
by the lead.

#### Scenario: No autonomous merge without authorization
- **WHEN** a PR is green but the owner has not pre-authorized merging it
- **THEN** the lead leaves it for the owner to merge and does not merge or close it

#### Scenario: Pre-authorized set merges on green
- **WHEN** the owner has explicitly pre-authorized merging a named set AND that change's gate is PASS, C is PASS, and roborev is clean
- **THEN** the lead MAY squash-merge it and run finalize

#### Scenario: Product and scope decisions escalate
- **WHEN** a product decision, a scope/title change, or an epic close is required
- **THEN** the lead escalates it to the owner and does not decide it autonomously

### Requirement: 1:1:1:1 state model
Each in-flight issue SHALL correspond to exactly one branch/worktree
(`issue-<N>-<slug>`), one OpenSpec change (`<slug>`), and one PR. The backlog
SHALL be GitHub issues + labels (one priority label, one lifecycle `status:*`
label).

#### Scenario: One-to-one artifacts per issue
- **WHEN** issue `#N` is in flight
- **THEN** exactly one worktree/branch `issue-<N>-<slug>`, one OpenSpec change `<slug>`, and one PR exist for it

### Requirement: Reuse CQLite's quality definition of done
A change driven through the pipeline SHALL be considered done only when
`agent-gate.sh` passes, the C intent audit reports PASS, and roborev is clean —
the same definition established by the `change-audit` capability. The pipeline
SHALL NOT introduce a weaker done bar.

#### Scenario: Done bar is gate + C + roborev
- **WHEN** the pipeline evaluates whether a change is done
- **THEN** it requires `agent-gate.sh` PASS AND C verdict PASS AND roborev clean before merge/finalize

### Requirement: Delivery telemetry ledger
The workflow SHALL maintain an append-only telemetry ledger at
`docs/reports/delivery-telemetry.jsonl` (one JSON record per line, one record per completed
issue) governed by a versioned JSON Schema at `docs/reports/delivery-telemetry.schema.json`.
A telemetry tool (`scripts/delivery-telemetry.py`) SHALL provide a `record` subcommand that
builds a schema-valid record and appends exactly one line, and a `lint` (alias `validate`)
subcommand that schema-validates every line and exits non-zero naming any malformed line.

#### Scenario: Record subcommand appends one schema-valid line
- **WHEN** `delivery-telemetry.py record` is run for a completed issue (GitHub-derived fields supplied via `--from-json` in tests, or pulled live from `gh`) with the required run counters
- **THEN** it appends exactly one JSON line to the ledger that validates against `delivery-telemetry.schema.json`
- **AND** the record carries the issue/PR numbers, routing, priority, the GitHub timestamps, the durations computed from those timestamps, and the supplied counters

#### Scenario: Lint rejects a malformed record
- **WHEN** `delivery-telemetry.py lint` runs against a ledger containing a line that violates the schema
- **THEN** it exits non-zero and names the offending line number
- **AND** a ledger whose lines all conform exits zero

### Requirement: Telemetry is authoritative data only
The ledger SHALL record only observed events — GitHub-sourced timestamps/labels and
run-observed counters explicitly supplied by the stamping step — and SHALL NOT infer,
estimate, or guess any value. Durations computed by arithmetic over authoritative timestamps
are permitted; a counter that was not observed SHALL NOT be defaulted to a fabricated value.

#### Scenario: Missing required counter is an error, not a silent zero
- **WHEN** `delivery-telemetry.py record` is invoked without a required run counter
- **THEN** it fails with an error rather than writing a record with an invented count
- **AND** every numeric field in a written record traces to a supplied counter or to arithmetic over GitHub-sourced timestamps

### Requirement: Finalize stamps the ledger
`flow-finalize` SHALL, as a step on a merged issue, write the issue's telemetry record by
invoking the `record` subcommand, so that every issue completed through the pipeline produces
exactly one ledger record.

#### Scenario: Finalize produces one record per completed issue
- **WHEN** `flow-finalize` completes for a merged issue
- **THEN** the ledger gains exactly one new record for that issue
- **AND** that record passes `lint`

### Requirement: Recurring retro ranks failures and files a deduped improvement issue
The workflow SHALL provide a `retro` subcommand that reads the ledger and the open
`flow-meta` issues, ranks the recorded failure categories by total recorded occurrences
weighted by a documented fixed weight table (a deterministic tally, not an inferred model),
and reports the single highest-cost recurring failure. By default it SHALL dry-run print the
ranked summary; with an explicit flag it SHALL file a `flow-meta` improvement issue, skipping
the filing when a matching open `flow-meta` issue already exists (dedupe). The manager
doctrine SHALL run this step on a cadence.

#### Scenario: Retro ranks a fixture ledger to the expected top failure
- **WHEN** `delivery-telemetry.py retro` runs against a fixture ledger whose dominant recorded failure category is known
- **THEN** it prints a ranked summary whose top entry is that category
- **AND** in the default mode it does not create any GitHub issue (dry-run)

#### Scenario: Retro dedupes against an existing flow-meta issue
- **WHEN** retro would file an improvement issue for a category that already has a matching open `flow-meta` issue (matched by a stable category marker)
- **THEN** it skips filing and reports that the category is already tracked

### Requirement: Telemetry tool is covered by a gate component
`scripts/agent-gate.sh` SHALL include a SKIP-aware `delivery-telemetry` component that runs
the telemetry tool's unit tests. The component SHALL record SKIP (loudly, never silent PASS)
when no `python3` is available and FAIL on any test failure.

#### Scenario: Gate runs the telemetry tests
- **WHEN** `scripts/agent-gate.sh --only delivery-telemetry` runs with `python3` available
- **THEN** it executes the telemetry unit tests and reports PASS only if they all pass
- **AND** with no `python3` it reports SKIP rather than PASS

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

