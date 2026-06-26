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

