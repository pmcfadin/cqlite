## ADDED Requirements

### Requirement: Shared claim board
The workflow SHALL maintain a shared cross-session board of in-flight work as a
GitHub Project (v2) with a single-select `Status` field
(`Backlog`/`Ready`/`In Progress`/`In Review`/`Done`). `flow-board` SHALL render the
board from the Project so any session (or human, including on mobile) sees what is
claimed and by whom.

#### Scenario: Board reflects claimed work with owner
- **WHEN** `flow-board` runs
- **THEN** it lists items by `Status` from the Project (`gh project item-list`)
- **AND** each `In Progress` item shows its assignee (the claiming session/owner)

### Requirement: Atomic-ish claim protocol prevents duplicate work
Before working an item, a session SHALL claim it so no two sessions work the same
item: it SHALL consider only **unassigned** `Ready` items, claim by adding itself as
assignee AND setting `Status` to `In Progress`, then **re-read** the item and proceed
ONLY if it is the assignee. If it is not the assignee (lost the race), it SHALL back
off and select the next `Ready` item.

#### Scenario: Two sessions race for the same item
- **WHEN** two independent sessions both attempt to claim the same `Ready` item
- **THEN** exactly one ends up as the assignee after the re-read
- **AND** the other detects it is not the assignee and moves to the next `Ready` item

#### Scenario: Already-claimed work is skipped
- **WHEN** a session selects the next item and the top `Ready` candidate is already assigned
- **THEN** the session does not claim or work it and considers the next unassigned `Ready` item

### Requirement: Stated concurrency model
The workflow SHALL document and follow a concurrency model: a single lead spawning
subagents is the default (the lead assigns disjoint work); multiple independent
sessions MUST use the claim protocol; Agent Teams is an optional desktop-only path.
Running multiple independent leads WITHOUT the claim protocol SHALL NOT be done.

#### Scenario: Single lead assigns disjoint work
- **WHEN** one lead spawns multiple subagents for one issue
- **THEN** the lead assigns each subagent distinct work and the subagents do not self-select overlapping work

#### Scenario: Independent sessions require the claim protocol
- **WHEN** more than one independent lead session operates on the backlog
- **THEN** each acquires work only through the claim protocol (assignee + re-read)

### Requirement: Mobile and remote operation
The pipeline SHALL be drivable from the Claude Code mobile app without running locally
on the phone: via Remote Control (the phone drives a local session) or via a Claude
Code on the web session (using the repo-committed `.claude/` plus a cloud setup script
that installs `openspec`/`gh` and fetches the dataset). The two human seams (spec
approval, PR merge) SHALL be performable from mobile via the session and GitHub.

#### Scenario: Drive the pipeline from mobile via Remote Control
- **WHEN** the owner runs `claude remote-control` on the laptop and connects from the mobile app
- **THEN** the full `flow-*` pipeline (worktrees, gate, `gh`, `openspec`) runs in that local session, driven from the phone

#### Scenario: Cloud session can run flow-implement
- **WHEN** a Claude Code on the web session runs the documented cloud setup script
- **THEN** `openspec` + `gh` are available and the dataset is fetched so `flow-implement` can run `agent-gate.sh`

### Requirement: Graceful degradation without the Project
The claim mechanism SHALL degrade to the existing label-based `status:*` model when
the `project` token scope or the board is unavailable, and SHALL NOT block work.

#### Scenario: Missing project scope falls back to labels
- **WHEN** the `project` token scope or the board is absent
- **THEN** `flow-*` uses `status:*` labels (and assignee) for claiming and continues without error
