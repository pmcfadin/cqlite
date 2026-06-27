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

### Requirement: Claim protocol prevents duplicate work across sessions and machines
Before working an item, a session SHALL claim it so no two sessions work the same
item — INCLUDING two sessions authenticated as the same GitHub user on different
machines. The session SHALL consider only items that are `Ready` AND have no existing
`issue-<N>-<slug>` branch on origin; it SHALL claim by **pushing the `issue-<N>-<slug>`
branch to origin (the cross-machine lock)** and setting assignee + `Status=In Progress`
(board visibility), then **re-read** and proceed ONLY if it holds the claim. If it lost
the race it SHALL back off and select the next eligible item. (Because the branch is on
origin, another machine MAY also resume an existing claim's work by fetching it.)

#### Scenario: Two sessions race for the same item
- **WHEN** two sessions both attempt to claim the same `Ready` item
- **THEN** exactly one successfully establishes the `issue-<N>-<slug>` branch on origin and proceeds
- **AND** the other detects the branch already exists (or it is not the holder on re-read) and moves to the next item

#### Scenario: Same user on two machines
- **WHEN** two sessions authenticated as the SAME GitHub user (on different machines) target the same item
- **THEN** the origin branch — not the shared assignee — is the deciding lock: only the machine whose branch is on origin proceeds
- **AND** the other machine skips it (and MAY fetch the branch to resume that work instead)

#### Scenario: Already-claimed work is skipped
- **WHEN** a session selects the next candidate and an `issue-<N>-<slug>` branch already exists on origin (or the item is not `Ready`)
- **THEN** the session does not claim or work it and considers the next eligible item

### Requirement: Board freshness and abandoned-claim recovery
The board SHALL stay current regardless of which client performed an action, and
abandoned claims SHALL be recoverable. Server-side GitHub Project automations SHALL
move items on GitHub-side events (e.g. PR merged or issue closed → `Done`). `flow-board`
SHALL reconcile drift and surface abandoned `In Progress` claims (an item `In Progress`
whose branch shows no recent progress) so they can be reclaimed or finished.

#### Scenario: GitHub-side action updates the board without an agent
- **WHEN** a PR is merged or its issue closed from the GitHub web/mobile UI (no `flow-*` run)
- **THEN** the Project automation moves the item to `Done`

#### Scenario: Abandoned claim is surfaced for recovery
- **WHEN** an item is `In Progress` but its `issue-<N>-<slug>` branch has had no recent commits (the claiming session died)
- **THEN** `flow-board` flags it as a stalled/abandoned claim so the owner can reclaim or finish it

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
